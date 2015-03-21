import unittest2 as unittest

from gcloudoem import entity, properties, connect, Key


class Entity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connect("DATASET")

    def test_key(self):
        class TEntity(entity.Entity):
            pass

        t1 = TEntity(key=1)
        self.assertEqual(t1.key.kind, TEntity.__name__)
        self.assertEqual(t1.key.id, 1)
        self.assertEqual(t1.key.name_or_id, 1)
        self.assertIsNone(t1.key.parent)
        self.assertFalse(t1.key.is_partial)
        self.assertIsInstance(t1.key, Key)
        self.assertIsInstance(TEntity.key, properties.KeyProperty)

        t1 = TEntity(key='abc')
        self.assertEqual(t1.key.name, 'abc')
        self.assertEqual(t1.key.name_or_id, 'abc')
        self.assertIsNone(t1.key.parent)
        self.assertFalse(t1.key.is_partial)

        t2 = TEntity(key=(t1, 1))
        self.assertEqual(t2.key.id, 1)
        self.assertEqual(t2.key.name_or_id, 1)
        self.assertIs(t2.key.parent, t1)

    def test_save(self):
        pass

    def test_validate(self):
        pass

    def test_property(self):
        class TEntity(entity.Entity):
            name = properties.TextProperty()

        t = TEntity()
        self.assertTrue(hasattr(t, 'name'))
        self.assertIsInstance(TEntity.name, properties.TextProperty)
        self.assertIsNone(t.name)
