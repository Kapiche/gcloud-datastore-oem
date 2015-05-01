# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# CHANGED BY Kapiche Ltd.
# Copyright (C) 2015 Kapiche Ltd. All rights reserved.
# Based on work by the good folk responsible for gcloud-python. Thanks folks!
# Author: Ryan Stuart<ryan@kapiche.com>
#
from __future__ import absolute_import, division, print_function, unicode_literals

from base64 import b64encode, b64decode

import unittest2

from gcloudoem import Entity, TextProperty, Key
from gcloudoem.datastore import datastore_v1_pb2 as datastore_pb, Connection
from gcloudoem.datastore.query import Query, Cursor
from gcloudoem.datastore.utils import prepare_key_for_request
from gcloudoem.exceptions import ConnectionError, InvalidQueryError

try:
    from unittest.mock import sentinel, patch, MagicMock, call
except ImportError:
    from mock import *


class TestQuery(unittest2.TestCase):
    class TestEntity(Entity):
        first_name = TextProperty()
        last_name = TextProperty()

    def test_query_no_connection(self):
        query = Query(self.TestEntity)
        self.assertRaises(ConnectionError, query)

    def test_query_init_no_entity(self):
        self.assertRaises(ValueError, Query, None)

    def test_query_init_default(self):
        query = Query(self.TestEntity)
        self.assertEqual(query.entity, self.TestEntity)
        self.assertEqual(query.ancestor, None)
        self.assertEqual(query.filters, [])
        self.assertEqual(query.projection, [])
        self.assertEqual(query.order, [])
        self.assertEqual(query.group_by, [])

    def test_query_init_args(self):
        ENTITY = self.TestEntity
        FILTERS = [('foo', '=', 'Qux'), ('bar', '<', 17)]
        PROJECTION = ['foo', 'bar', 'baz']
        ORDER = ['foo', 'bar']
        GROUP_BY = ['foo']
        query = Query(
            entity=ENTITY,
            filters=FILTERS,
            projection=PROJECTION,
            order=ORDER,
            group_by=GROUP_BY,
        )
        self.assertEqual(query.entity, self.TestEntity)
        self.assertEqual(query.filters, FILTERS)
        self.assertEqual(query.projection, PROJECTION)
        self.assertEqual(query.order, ORDER)
        self.assertEqual(query.group_by, GROUP_BY)

    def test_add_filter_setter_unknown_operator(self):
        query = Query(self.TestEntity)
        self.assertRaises(InvalidQueryError, query.add_filter, 'first_name', '~~', 'John')

    def test_add_filter_unknown_property(self):
        query = Query(self.TestEntity)
        self.assertRaises(InvalidQueryError, query.add_filter, 'name', '=', 'John')

    def test_add_filter_known_operator(self):
        query = Query(self.TestEntity)
        query.add_filter('first_name', '=', u'John')
        self.assertEqual(query.filters, [('first_name', '=', u'John')])

    def test_add_filter_all_binary_operators(self):
        query = Query(self.TestEntity)
        query.add_filter('first_name', '<=', u'val1')
        query.add_filter('first_name', '>=', u'val2')
        query.add_filter('first_name', '<', u'val3')
        query.add_filter('first_name', '>', u'val4')
        query.add_filter('first_name', '=', u'val5')
        self.assertEqual(len(query.filters), 5)
        self.assertEqual(query.filters[0], ('first_name', '<=', u'val1'))
        self.assertEqual(query.filters[1], ('first_name', '>=', u'val2'))
        self.assertEqual(query.filters[2], ('first_name', '<', u'val3'))
        self.assertEqual(query.filters[3], ('first_name', '>', u'val4'))
        self.assertEqual(query.filters[4], ('first_name', '=', u'val5'))

    def test_add_filter_all_term_operators(self):
        query = Query(self.TestEntity)
        query.add_filter('first_name', 'lte', u'val1')
        query.add_filter('first_name', 'gte', u'val2')
        query.add_filter('first_name', 'lt', u'val3')
        query.add_filter('first_name', 'gt', u'val4')
        query.add_filter('first_name', 'eq', u'val5')
        self.assertEqual(len(query.filters), 5)
        self.assertEqual(query.filters[0], ('first_name', 'lte', u'val1'))
        self.assertEqual(query.filters[1], ('first_name', 'gte', u'val2'))
        self.assertEqual(query.filters[2], ('first_name', 'lt', u'val3'))
        self.assertEqual(query.filters[3], ('first_name', 'gt', u'val4'))
        self.assertEqual(query.filters[4], ('first_name', 'eq', u'val5'))

    def test_add_filter_key_valid_key(self):
        query = Query(self.TestEntity)
        key = Key('Foo')
        query.add_filter('key', '=', key)
        self.assertEqual(query.filters, [('key', '=', key)])

    def test_filter_key_invalid_operator(self):
        key = Key('Foo')
        query = Query(self.TestEntity)
        self.assertRaises(InvalidQueryError, query.add_filter, 'key', '<', key)

    def test_filter_key_invalid_value(self):
        query = Query(self.TestEntity)
        self.assertRaises(InvalidQueryError, query.add_filter, 'key', '=', None)

    def test_projection_setter_empty(self):
        query = Query(self.TestEntity)
        query.projection = []
        self.assertEqual(query.projection, [])

    def test_projection_setter_string(self):
        query = Query(self.TestEntity)
        query.projection = 'first_name'
        self.assertEqual(query.projection, ['first_name'])

    def test_projection_setter_invalid_property(self):
        query = Query(self.TestEntity)
        with self.assertRaises(InvalidQueryError):
            query.projection = 'blah'

    def test_projection_setter_multiple_calls(self):
        PROJ1 = ['first_name']
        PROJ2 = ['first_name', 'last_name']
        query = Query(self.TestEntity)
        query.projection = PROJ1
        self.assertEqual(query.projection, PROJ1)
        query.projection = PROJ2
        self.assertEqual(query.projection, PROJ2)

    def test_keys_only(self):
        query = Query(self.TestEntity)
        query.keys_only()
        self.assertEqual(query.projection, ['__key__'])

    def test_order_setter_empty(self):
        query = Query(self.TestEntity)
        query.order = []
        self.assertEqual(query.order, [])

    def test_order_setter_string(self):
        query = Query(self.TestEntity)
        query.order = 'first_name'
        self.assertEqual(query.order, ['first_name'])

    def test_order_setter_single_item_list_desc(self):
        query = Query(self.TestEntity)
        query.order = '-first_name'
        self.assertEqual(query.order, ['-first_name'])

    def test_order_setter_multiple(self):
        query = Query(self.TestEntity)
        query.order = ['first_name', '-last_name']
        self.assertEqual(query.order, ['first_name', '-last_name'])

    def test_order_setter_unknown_property(self):
        query = Query(self.TestEntity)
        with self.assertRaises(InvalidQueryError):
            query.order = 'blah'

    def test_group_by_setter_empty(self):
        query = Query(self.TestEntity)
        query.group_by = []
        self.assertEqual(query.group_by, [])

    def test_group_by_setter_string(self):
        query = Query(self.TestEntity)
        query.group_by = 'first_name'
        self.assertEqual(query.group_by, ['first_name'])

    def test_group_by_setter_non_empty(self):
        query = Query(self.TestEntity)
        query.group_by = ['first_name', 'last_name']
        self.assertEqual(query.group_by, ['first_name', 'last_name'])

    def test_group_by_multiple_calls(self):
        query = Query(self.TestEntity)
        GROUP_BY1 = ['first_name', 'last_name']
        GROUP_BY2 = ['first_name']
        query.group_by = GROUP_BY1
        self.assertEqual(query.group_by, GROUP_BY1)
        query.group_by = GROUP_BY2
        self.assertEqual(query.group_by, GROUP_BY2)

    def test_group_by_unknown_property(self):
        query = Query(self.TestEntity)
        with self.assertRaises(InvalidQueryError):
            query.group_by = 'blah'

    def test_execute_query(self):
        query = Query(self.TestEntity)

        with patch('gcloudoem.datastore.query.get_connection') as mock:
            cursor = query()

            mock.assert_called_with()
            self.assertTrue(cursor._query is query)
            self.assertEqual(cursor._limit, None)
            self.assertEqual(cursor._offset, 0)

    def test_query_pb_empty(self):
        pb = Query(self.TestEntity).to_protobuf()
        self.assertEqual(list(pb.projection), [])
        self.assertEqual([item.name for item in pb.kind], [self.TestEntity._meta.kind])
        self.assertEqual(list(pb.order), [])
        self.assertEqual(list(pb.group_by), [])
        self.assertEqual(pb.filter.property_filter.property.name, '')
        cfilter = pb.filter.composite_filter
        self.assertEqual(cfilter.operator, datastore_pb.CompositeFilter.AND)
        self.assertEqual(list(cfilter.filter), [])
        self.assertEqual(pb.start_cursor, b'')
        self.assertEqual(pb.end_cursor, b'')
        self.assertEqual(pb.limit, 0)
        self.assertEqual(pb.offset, 0)

    def test_query_pb_projection(self):
        query = Query(self.TestEntity)
        query.projection = 'first_name'
        self.assertEqual([item.property.name for item in query.to_protobuf().projection], ['first_name'])

    @unittest2.expectedFailure
    def test_ancestor(self):
        ancestor = Key('Ancestor', 123, dataset_id='DATASET')
        pb = self._callFUT(_Query(ancestor=ancestor))
        cfilter = pb.filter.composite_filter
        self.assertEqual(cfilter.operator, datastore_pb.CompositeFilter.AND)
        self.assertEqual(len(cfilter.filter), 1)
        pfilter = cfilter.filter[0].property_filter
        self.assertEqual(pfilter.property.name, '__key__')
        ancestor_pb = _prepare_key_for_request(ancestor.to_protobuf())
        self.assertEqual(pfilter.value.key_value, ancestor_pb)

    def test_query_pb_filter(self):
        query = Query(self.TestEntity, filters=[('first_name', '=', u'John')])
        query.OPERATORS = {
            '=': datastore_pb.PropertyFilter.EQUAL,
        }
        pb = query.to_protobuf()
        cfilter = pb.filter.composite_filter
        self.assertEqual(cfilter.operator, datastore_pb.CompositeFilter.AND)
        self.assertEqual(len(cfilter.filter), 1)
        pfilter = cfilter.filter[0].property_filter
        self.assertEqual(pfilter.property.name, 'first_name')
        self.assertEqual(pfilter.value.string_value, u'John')

    def test_query_pb_filter_key(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.namespace = 'TEST'
        with patch('gcloudoem.properties.get_connection', return_value=connection):
            key = Key(self.TestEntity._meta.kind, value=123)
            query = Query(self.TestEntity, filters=[('key', '=', key)])
            query.OPERATORS = {
                '=': datastore_pb.PropertyFilter.EQUAL,
            }
            pb = query.to_protobuf()
            cfilter = pb.filter.composite_filter
            self.assertEqual(cfilter.operator, datastore_pb.CompositeFilter.AND)
            self.assertEqual(len(cfilter.filter), 1)
            pfilter = cfilter.filter[0].property_filter
            self.assertEqual(pfilter.property.name, '__key__')
            key_pb = prepare_key_for_request(self.TestEntity._properties['key'].to_protobuf(key))
            self.assertEqual(pfilter.value.key_value, key_pb)

    def test_query_pb_order(self):
        pb = Query(self.TestEntity, order=['first_name', '-last_name']).to_protobuf()
        self.assertEqual([item.property.name for item in pb.order], ['first_name', 'last_name'])
        self.assertEqual(
            [item.direction for item in pb.order],
            [datastore_pb.PropertyOrder.ASCENDING, datastore_pb.PropertyOrder.DESCENDING]
        )

    def test_group_by(self):
        pb = Query(self.TestEntity, group_by=['first_name', 'last_name']).to_protobuf()
        self.assertEqual([item.name for item in pb.group_by], ['first_name', 'last_name'])


class TestCursor(unittest2.TestCase):
    class TestEntity(Entity):
        first_name = TextProperty()

    _DATASET = 'DATASET'
    _NAMESPACE = 'NAMESPACE'
    _ID = 1234
    _START = b'\x00'
    _END = b'\xFF'
    _MORE = datastore_pb.QueryResultBatch.NOT_FINISHED
    _NO_MORE = datastore_pb.QueryResultBatch.MORE_RESULTS_AFTER_LIMIT

    def _addQueryResults(self, cursor=_END, more=False):
        entity_pb = datastore_pb.Entity()
        entity_pb.key.partition_id.dataset_id = self._DATASET
        path_element = entity_pb.key.path_element.add()
        path_element.kind = self.TestEntity._meta.kind
        path_element.id = self._ID
        prop = entity_pb.property.add()
        prop.name = 'first_name'
        prop.value.string_value = u'Alice'
        return [entity_pb], cursor, self._MORE if more else self._NO_MORE

    def test_cursor_init(self):
        query = sentinel.query
        conn = sentinel.connection
        cursor = Cursor(query, conn)
        self.assertTrue(cursor._query is query)
        self.assertTrue(cursor._connection is conn)
        self.assertEqual(cursor._limit, None)
        self.assertEqual(cursor._offset, 0)

    def test_cursor_init_limits(self):
        connection = sentinel.connection
        query = sentinel.query
        cursor = Cursor(query, connection, 13, 29)
        self.assertTrue(cursor._query is query)
        self.assertTrue(cursor._connection is connection)
        self.assertEqual(cursor._limit, 13)
        self.assertEqual(cursor._offset, 29)

    def test_next_page_no_cursors_no_more(self):
        attrs = {'run_query.return_value': self._addQueryResults()}
        connection = MagicMock(spec=Connection, **attrs)
        connection.namespace = self._NAMESPACE
        query = Query(self.TestEntity)
        cursor = Cursor(query, connection)
        entities, more_results, position = cursor.next_page()

        self.assertEqual(position, b64encode(self._END))
        self.assertFalse(more_results)
        self.assertFalse(cursor._more_results)
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].key.name_or_id, self._ID)
        self.assertEqual(entities[0].first_name, 'Alice')

        qpb = query.to_protobuf()
        qpb.offset = 0
        self.assertEqual(
            connection.method_calls,
            [call.run_query(query_pb=qpb, namespace=self._NAMESPACE, transaction_id=None,)]
        )

    def test_next_page_no_cursors_no_more_w_offset_and_limit(self):
        attrs = {'run_query.return_value': self._addQueryResults()}
        connection = MagicMock(spec=Connection, **attrs)
        connection.namespace = self._NAMESPACE
        query = Query(self.TestEntity)
        cursor = Cursor(query, connection, limit=13, offset=29)
        entities, more_results, position = cursor.next_page()

        self.assertEqual(position, b64encode(self._END))
        self.assertFalse(more_results)
        self.assertFalse(cursor._more_results)
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].key.name_or_id, self._ID)
        self.assertEqual(entities[0].first_name, 'Alice')

        qpb = query.to_protobuf()
        qpb.limit = 13
        qpb.offset = 29
        self.assertEqual(
            connection.method_calls,
            [call.run_query(query_pb=qpb, namespace=self._NAMESPACE, transaction_id=None,)]
        )

    def test_next_page_w_cursors_w_more(self):
        attrs = {'run_query.return_value': self._addQueryResults(more=True)}
        connection = MagicMock(spec=Connection, **attrs)
        connection.namespace = self._NAMESPACE
        query = Query(self.TestEntity)
        cursor = Cursor(query, connection, start_cursor=self._START, end_cursor=self._END)
        entities, more_results, position = cursor.next_page()

        self.assertEqual(position, b64encode(self._END))
        self.assertTrue(more_results)
        self.assertTrue(cursor._more_results)
        self.assertEqual(cursor._end_cursor, None)
        self.assertEqual(b64decode(cursor._start_cursor), self._END)
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].key.name_or_id, self._ID)
        self.assertEqual(entities[0].first_name, 'Alice')

        qpb = query.to_protobuf()
        qpb.offset = 0
        qpb.start_cursor = b64decode(self._START)
        qpb.end_cursor = b64decode(self._END)
        self.assertEqual(
            connection.method_calls,
            [call.run_query(query_pb=qpb, namespace=self._NAMESPACE, transaction_id=None,)]
        )

    def test_next_page_w_cursors_w_bogus_more(self):
        epb, position, _ = self._addQueryResults(cursor=self._END, more=True)
        attrs = {'run_query.return_value': (epb, position, 4)}
        connection = MagicMock(spec=Connection, **attrs)
        query = Query(self.TestEntity)
        cursor = Cursor(query, connection)
        self.assertRaises(RuntimeError, cursor.next_page)

    def test___iter___no_more(self):
        attrs = {'run_query.return_value': self._addQueryResults()}
        connection = MagicMock(spec=Connection, **attrs)
        connection.namespace = self._NAMESPACE
        query = Query(self.TestEntity)
        cursor = Cursor(query, connection)
        entities = list(cursor)

        self.assertFalse(cursor._more_results)
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].key.name_or_id, self._ID)
        self.assertEqual(entities[0].first_name, 'Alice')

        qpb = query.to_protobuf()
        qpb.offset = 0
        self.assertEqual(
            connection.method_calls,
            [call.run_query(query_pb=qpb, namespace=self._NAMESPACE, transaction_id=None,)]
        )

    def test___iter___w_more(self):
        attrs = {'run_query.return_value': self._addQueryResults(cursor=self._END, more=True)}
        connection = MagicMock(spec=Connection, **attrs)
        connection.namespace = self._NAMESPACE
        query = Query(self.TestEntity)
        cursor = Cursor(query, connection)
        it = iter(cursor)

        entity1 = next(it)
        self.assertTrue(cursor._more_results)
        self.assertEqual(entity1.key.name_or_id, self._ID)
        self.assertEqual(entity1.first_name, 'Alice')

        connection.configure_mock(**{
            'run_query.return_value': self._addQueryResults(),
            'dataset.return_vlaue': self._DATASET
        })
        entity2 = next(it)
        self.assertFalse(cursor._more_results)
        self.assertEqual(entity2.key.name_or_id, self._ID)
        self.assertEqual(entity2.first_name, 'Alice')
        self.assertRaises(StopIteration, next, it)

        qpb1 = query.to_protobuf()
        qpb1.offset = 0
        qpb2 = query.to_protobuf()
        qpb2.offset = 0
        qpb2.start_cursor = self._END
        self.assertEqual(
            connection.method_calls,
            [
                call.run_query(query_pb=qpb1, namespace=self._NAMESPACE, transaction_id=None,),
                call.run_query(query_pb=qpb2, namespace=self._NAMESPACE, transaction_id=None,)
            ]
        )
