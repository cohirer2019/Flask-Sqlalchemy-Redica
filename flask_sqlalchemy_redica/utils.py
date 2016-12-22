# -*- coding: utf-8 -*-
import hashlib

from flask import current_app
from werkzeug.local import LocalProxy


def _md5_key_mangler(prefix, key):
    if key.startswith('SELECT '):
        key = hashlib.md5(key.encode('utf-8')).hexdigest()
    return ':'.join([prefix, key])


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


def _get_current_redica():
    if hasattr(current_app, 'extensions'):
        return current_app.extensions['sqlalchemy_redica']

current_redica = LocalProxy(lambda: _get_current_redica())
