# -*- coding: utf-8 -*-
import functools

from flask_sqlalchemy import BaseQuery
from sqlalchemy.orm.interfaces import MapperOption
from dogpile.cache.api import NO_VALUE

from .utils import _prefixed_key_from_query, _key_from_query, current_redica


class CachingQuery(BaseQuery):

    def __init__(self, regions, *args, **kwargs):
        self.cache_regions = regions
        super(CachingQuery, self).__init__(*args, **kwargs)

    def __iter__(self):
        if hasattr(self, '_cache_region'):
            expiration_time = self._cache_region.expiration_time
            return self.get_value(
                createfunc=lambda: list(super(CachingQuery, self).__iter__()),
                expiration_time=expiration_time
            )
        else:
            return super(CachingQuery, self).__iter__()

    def _get_cache_plus_key(self):
        if not self.cache_regions:
            self.cache_regions = current_redica.regions
        dogpile_region = self.cache_regions[self._cache_region.region]
        if self._cache_region.cache_key:
            key = self._cache_region.cache_key
        elif self._cache_region.query_prefix:
            key = _prefixed_key_from_query(
                self, self._cache_region.query_prefix)
        else:
            key = _key_from_query(self)
        return dogpile_region, key

    def invalidated(self):
        dogpile_region, cache_key = self._get_cache_plus_key()
        dogpile_region.delete(cache_key)

    def get_value(self, merge=True, createfunc=None, expiration_time=None,
                  ignore_expiration=False):
        dogpile_region, cache_key = self._get_cache_plus_key()

        assert not ignore_expiration or not createfunc, \
            "Can't ignore expiration and also provide createfunc"

        if ignore_expiration or not createfunc:
            cached_value = dogpile_region.get(
                cache_key, expiration_time=expiration_time,
                ignore_expiration=ignore_expiration)
        else:
            cached_value = dogpile_region.get_or_create(
                cache_key, createfunc, expiration_time=expiration_time)

        if cached_value is NO_VALUE:
            raise KeyError(cache_key)
        if merge:
            cached_value = self.merge_result(cached_value, load=False)

        return cached_value

    def set_value(self, value):
        dogpile_region, cache_key = self._get_cache_plus_key()
        dogpile_region.set(cache_key, value)


def query_callable(regions, query_cls=CachingQuery):
    return functools.partial(query_cls, regions)


class FromCache(MapperOption):

    propagate_to_loaders = False

    def __init__(self, region='default', cache_key=None, query_prefix=None,
                 cache_regions=None, expiration_time=None):
        self.region = region
        self.cache_key = cache_key
        self.query_prefix = query_prefix
        self.cache_regions = cache_regions
        self.expiration_time = expiration_time

    def process_query(self, query):
        query._cache_region = self
        if self.cache_regions:
            query.cache_regions = self.cache_regions


class RelationshipCache(MapperOption):
    """Specifies that a Query as called within a "lazy load"
       should load results from a cache."""

    propagate_to_loaders = True

    def __init__(self, attribute, region="default", cache_key=None):
        """Construct a new RelationshipCache.

        :param attribute: A Class.attribute which
        indicates a particular class relationship() whose
        lazy loader should be pulled from the cache.

        :param region: name of the cache region.

        :param cache_key: optional.  A string cache key
        that will serve as the key to the query, bypassing
        the usual means of forming a key from the Query itself.

        """
        self.region = region
        self.cache_key = cache_key
        self._relationship_options = {
            (attribute.property.parent.class_, attribute.property.key): self
        }

    def process_query_conditionally(self, query):
        """Process a Query that is used within a lazy loader.

        (the process_query_conditionally() method is a SQLAlchemy
        hook invoked only within lazyload.)

        """
        if query._current_path:
            mapper, prop = query._current_path[-2:]
            key = prop.key

            for cls in mapper.class_.__mro__:
                if (cls, key) in self._relationship_options:
                    relationship_option = self._relationship_options[(cls, key)]
                    query._cache_region = relationship_option
                    break

    def and_(self, option):
        """Chain another RelationshipCache option to this one.

        While many RelationshipCache objects can be specified on a single
        Query separately, chaining them together allows for a more efficient
        lookup during load.

        """
        self._relationship_options.update(option._relationship_options)
        return self
