# -*- coding: utf-8 -*-
import hashlib


def md5_key_mangler(prefix_func, key):
    if key.startswith('SELECT '):
        key = hashlib.md5(key.encode('utf-8')).hexdigest()
    return prefix_func(key) if prefix_func else key


def _key_from_query(query):
    stmt = query.with_labels().statement
    compiled = stmt.compile()
    params = compiled.params

    return ' '.join(
        [unicode(compiled)] + [unicode(params[k]) for k in sorted(params)]
    )


def _prefixed_key_from_query(query, prefix):
    key = _key_from_query(query)
    return ':'.join([prefix, hashlib.md5(key.encode('utf-8')).hexdigest()])
