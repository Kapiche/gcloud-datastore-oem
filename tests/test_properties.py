import six
import unittest2 as unittest

from gcloudoem import entity, properties, connect
from gcloudoem.queryset.errors import ValidationError


class Properties(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connect("DATASET")

    def test_BooleanProperty(self):
        class TEntity(entity.Entity):
            test_bool = properties.BooleanProperty()

        e = TEntity()
        self.assertIsNone(e.test_bool)

        e = TEntity(test_bool=False)
        self.assertFalse(e.test_bool)

        e.test_bool = True
        self.assertTrue(e.test_bool)

        class TEntity(entity.Entity):
            test_bool = properties.BooleanProperty(default=True)

        e = TEntity()
        self.assertTrue(e.test_bool)
        self.assertIsInstance(TEntity.test_bool, properties.BooleanProperty)

        TEntity.test_bool._validate(True)
        self.assertRaises(ValidationError, TEntity.test_bool._validate, 1)

    def test_IntegerProperty(self):
        class TEntity(entity.Entity):
            test_int = properties.IntegerProperty()

        e = TEntity()
        self.assertIsNone(e.test_int)

        class TEntity(entity.Entity):
            test_int = properties.IntegerProperty(default=3)

        e = TEntity()
        self.assertEqual(e.test_int, 3)
        e.test_int = 4
        self.assertEqual(e.test_int, 4)
        self.assertIsInstance(TEntity.test_int, properties.IntegerProperty)

        TEntity.test_int._validate(1)
        self.assertRaises(ValidationError, TEntity.test_int._validate, '')

    def test_FloatProperty(self):
        class TEntity(entity.Entity):
            test_float = properties.FloatProperty()

        e = TEntity()
        self.assertIsNone(e.test_float)

        class TEntity(entity.Entity):
            test_float = properties.FloatProperty(default=0.1)

        e = TEntity()
        self.assertEqual(e.test_float, 0.1)
        e.test_float = 0.2
        self.assertEqual(e.test_float, 0.2)
        self.assertIsInstance(TEntity.test_float, properties.FloatProperty)

        TEntity.test_float._validate(1.1)
        self.assertRaises(ValidationError, TEntity.test_float._validate, '')

    def test_TextProperty(self):
        class TEntity(entity.Entity):
            test_text = properties.TextProperty()

        e = TEntity()
        self.assertIsNone(e.test_text)

        class TEntity(entity.Entity):
            test_text = properties.TextProperty(default="")

        e = TEntity()
        self.assertEqual(e.test_text, "")

        class TEntity(entity.Entity):
            test_text = properties.TextProperty(default=lambda: "")

        e = TEntity()
        self.assertEqual(e.test_text, "")
        self.assertIsInstance(TEntity.test_text, properties.TextProperty)

        TEntity.test_text._validate('abc')
        self.assertRaises(ValidationError, TEntity.test_text._validate, b'blah')

    def test_PickleProperty(self):
        class TEntity(entity.Entity):
            test_pickle = properties.PickleProperty()

        e = TEntity()
        self.assertIsNone(e.test_pickle)

        e = TEntity(test_pickle={"123": "456"})
        self.assertEqual(e.test_pickle, {"123": "456"})
        e.test_pickle = {'456': '789'}
        self.assertEqual(e.test_pickle, {'456': '789'})
        self.assertIsInstance(TEntity.test_pickle, properties.PickleProperty)

    def test_JsonProperty(self):
        class TEntity(entity.Entity):
            test_json = properties.JsonProperty()

        e = TEntity()
        self.assertIsNone(e.test_json)

        e = TEntity(test_json={"123": "456"})
        self.assertEqual(e.test_json, {"123": "456"})
        e.test_json = {'456': '789'}
        self.assertEqual(e.test_json, {'456': '789'})
        self.assertIsInstance(TEntity.test_json, properties.JsonProperty)

    def test_DataTimeProperty(self):
        import datetime

        class TEntity(entity.Entity):
            test_datetime = properties.DateTimeProperty()

        e = TEntity()
        self.assertIsNone(e.test_datetime)

        utcnow = datetime.datetime.utcnow()
        e.test_datetime = utcnow
        self.assertEqual(e.test_datetime, utcnow)
        self.assertIsInstance(TEntity.test_datetime, properties.DateTimeProperty)

        class TEntity(entity.Entity):
            test_datetime = properties.DateTimeProperty(default=utcnow)

        e = TEntity()
        self.assertEqual(e.test_datetime, utcnow)

        TEntity.test_datetime._validate(utcnow)
        self.assertRaises(ValidationError, TEntity.test_datetime._validate, False)

    def test_DateProperty(self):
        import datetime

        class TEntity(entity.Entity):
            test_date = properties.DateProperty()

        e = TEntity()
        self.assertIsNone(e.test_date)

        today = datetime.date.today()
        e.test_date = today
        self.assertEqual(e.test_date, today)

        self.assertIsInstance(TEntity.test_date, properties.DateProperty)

        TEntity.test_date._validate(today)
        self.assertRaises(ValidationError, TEntity.test_date._validate, False)

    def test_TimeProperty(self):
        import datetime

        class TEntity(entity.Entity):
            test_time = properties.TimeProperty()

        e = TEntity()
        self.assertIsNone(e.test_time)

        t = datetime.time()
        e.test_time = t
        self.assertEqual(e.test_time, t)
        self.assertIsInstance(TEntity.test_time, properties.TimeProperty)

        TEntity.test_time._validate(t)
        self.assertRaises(ValidationError, TEntity.test_time._validate, False)
