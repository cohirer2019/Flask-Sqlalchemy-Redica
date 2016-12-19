# coding: UTF-8
import unittest

from flask import Flask
from flask_sqlalchemy_redica import CachingSQLAlchemy


db = CachingSQLAlchemy()


class Country(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


def create_app():
    app = Flask(__name__)
    app.config['CACHE_TYPE'] = 'simple'
    db.init_app(app)
    return app


class TestFromCache(unittest.TestCase):

    def setUp(self):
        self.app = create_app()
        self.ctx = self.app.app_context()
        self.ctx.push()
        db.create_all()
        db.session.add(Country(name='Brazil'))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.ctx.pop()

    def test_cache_hit(self):
        q = Country.query.order_by(Country.name.desc())
        caching_q = q.options(FromCache(cache))

        # cache miss
        country = caching_q.first()
        self.assertEqual('Brazil', country.name)

        # add another record
        c = Country(name='Germany')
        db.session.add(c)
        db.session.commit()

        # no cache used
        self.assertEqual('Germany', q.first().name)

        # cache hit
        self.assertEqual('Brazil', caching_q.first().name)