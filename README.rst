Google Datastore Object-Entity-Mapper
=====================================
Python object to entity mapper for Google Datastore with Django support.

Quickstart
----------
::

    from gcloudoem import *

    class TestEntity(Entity):
        name = TextProperty()


    class OtherTestEntity(Entity):
        address = TextProperty(default='blah')
        te = ListProperty(ReferenceProperty(TestEntity))

    connect('dataset_id')

    es = TestEntity.objects.filter(name='Alice')
    oe = OtherTestEntity(te=[e for e in es])
    oe.save()
    ot = OtherTestEntity.objects.get(pk=oe.key.name_or_id)
    print(ot.key.name_or_id, [te.name for te in ot.te])
    query = query.Query(TestEntity)
    query.add_filter("name", "=", "Kris")
    cursor = query()
    print([(e.name, e.key.name_or_id,) for e in list(o)])

Copyright and License
---------------------
GCloudOEM is Copyright (C) the respective authors of the files, as noted at the top of each file. If not noted, it is
Copyright (C) 2015 Kapiche Ltd. All original code in Bugeye is licensed under the `GNU Affero General Public License
<http://scraper-helper.sourceforge.net/agpl-3.0.txt>`_.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public
License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
version.

The copyright holders grant you an additional permission under Section 7 of the GNU Affero General Public License,
version 3, exempting you from the requirement in Section 6 of the GNU General Public License, version 3, to accompany
Corresponding Source with Installation Information for the Program or any work based on the Program. You are still
required to comply with all other Section 6 requirements to provide Corresponding Source.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
details.
