import gcloudoem
from gcloudoem import entity, properties
from gcloudoem.datastore.query import Query


class TestEntity(entity.Entity):
    name = properties.TextProperty()

gcloudoem.connect('research-by-kapiche')

e = TestEntity(name='Alice')
# e.save()
# print(e, e.key.name_or_id)
query = Query(TestEntity)
query.add_filter("name", "=", "Alice")
cursor = query()
print(list(cursor))
