#!/usr/bin/env python
"""
Flask-SQLAlchemy-Redica
----------------
Adds Caching support to your Flask SQLAlchemy.

"""
from setuptools import setup

setup(
    name='Flask-SQLAlchemy-Redica',
    version='3.0',
    url='http://dev.smart4e.com/oceanio/flask-sqlalchemy-redica',
    license='BSD',
    author='Alan Zhang',
    author_email='oceanio@gmail.com',
    maintainer='Alan Zhang',
    maintainer_email='oceanio@gmail.com',
    description='Adds Caching support to your Flask SQLAlchemy',
    long_description=__doc__,
    packages=['flask_sqlalchemy_redica'],
    zip_safe=False,
    platforms='any',
    install_requires=[
        'Flask-SQLAlchemy>=2.0,<=3.0',
        'dogpile.cache>=0.6.2'
    ],
    test_suite='tests',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ]
)