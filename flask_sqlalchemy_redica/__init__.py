# -*- coding: utf-8 -*-

"""
    flaskext.sqlalchemy_caching
    ~~~~~~~~~~~~~~~~~~~
    Adds basic SQLAlchemy cache support to your application.
    :copyright: (c) 2017 by oceanio@gmail.com
    :license: BSD, see LICENSE for more details.
"""

from .redis import ExtendRedisBackend
from .core import CachingSQLAlchemy
