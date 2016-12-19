# -*- coding: utf-8 -*-
import itertools

import functools
from blinker import signal
from dogpile.cache.api import NO_VALUE
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import event, inspect
from sqlalchemy.orm.attributes import get_history
from sqlalchemy.orm.base import PASSIVE_NO_INITIALIZE

from .cache import FromCache


class Cache(object):
    def __init__(self, model, regions, label, exclude_columns=None):
        self.model = model
        self.regions = regions
        self.label = label
        self.pk = getattr(model, 'cache_pk', 'id')
        self.exclude_columns = set(exclude_columns) \
            if exclude_columns else set()
        self.columns = None

    def get(self, pk):
        return self.model.query.options(self.from_cache(pk=pk)).get(pk)

    def filter(self, **kwargs):
        limit = kwargs.pop('limit', None)
        offset = kwargs.pop('offset', None)
        order_by = kwargs.pop('order_by', 'asc')

        query_kwargs = {}
        if kwargs:
            if len(kwargs) > 1:
                raise TypeError(
                    'filter accept only one attribute for filtering')
            key, value = kwargs.items()[0]
            if key == self.pk:
                yield self.get(value)
                return

            if key not in self._columns():
                raise TypeError('%s does not have an attribute %s' % self, key)
            query_kwargs[key] = value

        cache_key = self.cache_key(**kwargs)
        pks = self.regions[self.label].get(cache_key)

        if pks is NO_VALUE:
            pks = [o.id for o in self.model.query.filter_by(
                **query_kwargs).with_entities(getattr(self.model, self.pk))]
            self.regions[self.label].set(cache_key, pks)

        if order_by == 'desc':
            pks.reverse()

        if offset is not None:
            pks = pks[pks:]

        if limit is not None:
            pks = pks[:limit]

        keys = [self.cache_key(pk) for pk in pks]
        for pos, obj in enumerate(self.regions[self.label].get_multi(keys)):
            if obj is NO_VALUE:
                yield self.get(pks[pos])
            else:
                yield obj[0]

    def flush(self, key):
        self.regions[self.label].delete(key)

    def keys(self, key_pattern):
        return self.regions[self.label].backend.keys(key_pattern)

    def flush_multi(self, key_pattern):
        if not key_pattern.endswith('*'):
            key_pattern += '*'
        backend = self.regions[self.label].backend
        keys = backend.keys(key_pattern)
        if len(keys) > 0:
            backend.delete_multi(keys)

    def _columns(self):
        if not self.columns:
            self.columns = [
                c.name for c in self.model.__table__.columns
                if c.name != self.pk and c.name not in self.exclude_columns]

        return self.columns

    def from_cache(self, cache_key=None, pk=None, prefix=None):
        if pk:
            cache_key = self.cache_key(pk)
        return FromCache(self.label, cache_key, query_prefix=prefix)

    def cache_key(self, pk='all', **kwargs):
        q_filter = ''.join('{}={}'.format(k, v) for k, v in kwargs.items()) \
            or self.pk
        return "{}:{}:object:{}".format(self.model.__table__, pk, q_filter)

    def cache_relationship_key(self, pk, relation_name):
        return '{}:{}:relationship:{}'.format(
            self.model.__tablename__, pk, relation_name)

    def cache_query_key(self, pk, query_name):
        return '{}:{}:query:{}'.format(self.model.__tablename__, pk, query_name)

    def flush_filters(self, obj):
        for column in self._columns():
            added, _, deleted = get_history(
                obj, column, passive=PASSIVE_NO_INITIALIZE)
            for value in itertools.chain(added or (), deleted or ()):
                self.flush(self.cache_key(**{column: value}))

    def flush_caches(self, obj_pk):
        self.flush(self.cache_key(obj_pk))
        self.flush_multi(self.cache_relationship_key(obj_pk, '*'))
        self.flush_multi(self.cache_query_key(obj_pk, '*'))

    def flush_all(self, obj):
        self.flush_filters(obj)
        self.flush(self.cache_key())

        obj_pk = getattr(obj, self.pk)
        if obj_pk:
            self.flush_caches(obj_pk)


flush_signal = signal('_flask_sqlalchemy_redica_flush')


class SignalMixin(object):
    receive_signal = True

    # these columns changes will not produce cache flush
    exclude_listen_columns = {'update_at', 'create_at'}
    cached_notify_relationships = ()

    _initialized = False
    _listen_columns = set()
    _all_columns = set()

    @classmethod
    def init_listen_columns(cls, mapper):
        cls._all_columns = set(mapper.attrs.keys())
        cls._listen_columns = \
            cls._listen_columns or cls._all_columns - cls.exclude_listen_columns

    @classmethod
    def has_changes(cls, target, use_all=False):
        columns = cls._all_columns if use_all else cls._listen_columns
        for column in columns:
            if get_history(target, column,
                           passive=PASSIVE_NO_INITIALIZE).has_changes():
                return True

    @classmethod
    def relation_changes(cls, target, attr, r):
        history = get_history(target, r)
        if attr.back_populates \
                and attr.cascade_backrefs \
                and history.has_changes():
            # backref can update itself
            # no need to broadcast signals
            dataset = history.unchanged or ()
        else:
            dataset = history.sum()

        for obj in dataset:
            if not obj or obj.id is None:
                # for new obj, it will flush by itself,
                # no need to broadcast signal
                continue

            yield obj

    @classmethod
    def _notify_all(cls, target, ev, delay=False, indent=4):
        if not target:
            return

        mapper = inspect(cls).mapper
        for r in cls.cached_notify_relationships:
            attr = mapper.attrs.get(r)
            sender = attr.mapper.class_.__name__
            for obj in cls.relation_changes(target, attr, r):
                if delay:
                    cache_invalidator.check_item(
                        [ev, attr.mapper.class_.__module__, sender, obj.id])
                else:
                    flush_signal.send(
                        sender, target=obj, event=ev, indent=indent+4)

    @classmethod
    def receive_flush_signal(cls, sender, **kw):
        target = kw.get('target')
        src = kw.get('src')
        ev = kw.get('event')
        indent = kw.get('indent', 4)

        if src == 'on_model_changed' \
                and ev == 'update' \
                and not cls.has_changes(target):
            # for self update, if no changes, then do not notify others
            return

        delay = True
        if ev == 'delete' or src != 'on_model_changed':
            # deletion need flush right away
            delay = False

        cls._notify_all(target, ev, delay=delay, indent=indent)

    @classmethod
    def listen_mapper_events(cls, mapper, sender, callback):
        object_update_callback = functools.partial(
            callback, sender, 'update')
        object_delete_callback = functools.partial(
            callback, sender, 'delete')
        object_insert_callback = functools.partial(
            callback, sender, 'insert')
        event.listen(mapper, 'after_update', object_update_callback)
        event.listen(mapper, 'after_delete', object_delete_callback)
        event.listen(mapper, 'after_insert', object_insert_callback)

    @classmethod
    def on_model_changed(cls, *args):
        sender, ev, _, _, target = args
        flush_signal.send(
            sender, target=target, event=ev, src='on_model_changed')

    @classmethod
    def __declare_last__(cls):
        if not cls.receive_signal or cls._initialized:
            return

        mapper = inspect(cls).mapper
        sender = mapper.class_.__name__
        cls.listen_mapper_events(mapper, sender, cls.on_model_changed)
        flush_signal.connect(
            cls.receive_flush_signal, sender=sender, weak=False)

        base_mapper = inspect(cls).mapper.base_mapper
        if base_mapper != mapper:
            sender = base_mapper.class_.__name__
            cls.listen_mapper_events(base_mapper, sender, cls.on_model_changed)
            flush_signal.connect(
                cls.receive_flush_signal, sender=sender, weak=False)

        cls.init_listen_columns(mapper)
        cls._initialized = True


class CacheableMixin(SignalMixin):
    cacheable = True
    cache_label = 'default'
    cache_regions = None
    cached_queries = ()
    cached_relationships = ()
    # these columns will not produce cache indices
    cache_exclude_columns = ()

    @declared_attr
    def cache(cls):
        return Cache(
            cls, cls.cache_regions, cls.cache_label,
            exclude_columns=cls.cache_exclude_columns
        )

    @classmethod
    def _flush_all(cls, target_id, target, indent=4):
        if not cls.cacheable:
            return

        if target:
            cls.cache.flush_all(target, indent)
        elif target_id:
            cls.cache.flush_caches(target_id, indent)

    @classmethod
    def relationship_cache_key(cls, pk, relation_name):
        return cls.cache.cache_relationship_key(pk, relation_name)

    @classmethod
    def query_cache_key(cls, pk, query_name):
        return cls.cache.cache_query_key(pk, query_name)

    @classmethod
    def receive_flush_signal(cls, sender, **kw):
        super(CacheableMixin, cls).receive_flush_signal(sender, **kw)
        target = kw.get('target')
        target_id = kw.get('target_id')
        indent = kw.get('indent', 4)
        cls._flush_all(target_id, target, indent)

    @classmethod
    def __declare_last__(cls):
        if cls.cacheable:
            super(CacheableMixin, cls).__declare_last__()


def get_check_list(instance):
    key = '_delay_checker_check_list_{}'.format(id(instance))
    check_list = getattr(g, key, None)
    if check_list is None:
        check_list = []
        setattr(g, key, check_list)
    return check_list


class DelayedChecker(object):
    ''' DelayedChecker is used for tracking db changes after data model
        committed. It pushes check_mothod to celery queus so that no main
        thread performance would be affected.
    '''
    def __init__(
            self, check_method, stamp_time=False, batch=False,
            immediate=False):

        assert 'check_method specified as string can\'t be called ' \
               'immediately', not isinstance(check_method, basestring) or \
                              not immediate

        self.check_list = LocalProxy(lambda: get_check_list(self))
        self.check_method = check_method
        self.stamp_time = stamp_time
        self.batch = batch
        self.immediate = immediate

    def check_item(self, info):
        if info and info not in self.check_list:
            self.check_list.append(info)

    def call_check_method(self, *args, **kw):
        if isinstance(self.check_method, basestring):
            celery.send_task(self.check_method, args=args, kwargs=kw)
        elif self.immediate:
            self.check_method(*args, **kw)
        else:
            self.check_method.delay(*args, **kw)

    def flush(self, *args, **kwargs):
        if not self.check_list:
            return

        if self.stamp_time:
            # flush_at is mainly called for check_method to compare with
            # update_at field of the object / objects to determine whether
            # the real action need to take place
            kwargs['flushed_at'] = utcnow()

        check_list = self.check_list[:]
        self.check_list = []
        if self.batch:
            self.call_check_method(check_list, **kwargs)
        else:
            map(lambda info: self.call_check_method(
                info, **kwargs), check_list)