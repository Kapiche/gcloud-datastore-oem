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
from unittest.mock import MagicMock, patch

import unittest2

from gcloudoem import Transaction, Entity, TextProperty, KeyProperty
from gcloudoem.datastore import Connection, datastore_v1_pb2 as datastore_pb
from gcloudoem.exceptions import ConnectionError


class TestTransaction(unittest2.TestCase):
    class TestEntity(Entity):
        first_name = TextProperty()

    def _make_key_pb(self, id=1234):
        from gcloudoem.key import Key
        k = KeyProperty()
        return k.to_protobuf(Key('Kind', value=id))

    def test_txn_init_missing_required(self):
        with self.assertRaises(ConnectionError):
            Transaction(Transaction.SNAPSHOT)

    def test_txn_init(self):
        from gcloudoem.datastore.datastore_v1_pb2 import Mutation

        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SNAPSHOT)
            self.assertTrue(mock.called)
            self.assertEqual(txn._isolation, Transaction.SNAPSHOT)
            self.assertEqual(txn._connection, connection)
            self.assertEqual(txn.id, None)
            self.assertEqual(txn._status, Transaction._INITIAL)
            self.assertIsInstance(txn._mutation, Mutation)
            self.assertEqual(len(txn._auto_id_entities), 0)

    def test_txn_current(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn1 = Transaction(Transaction.SNAPSHOT)
            txn2 = Transaction(Transaction.SNAPSHOT)
            self.assertTrue(mock.called)
            self.assertTrue(txn1.current() is None)
            self.assertTrue(txn2.current() is None)
            with txn1:
                self.assertTrue(txn1.current() is txn1)
                self.assertTrue(txn2.current() is txn1)
                with txn2:
                    self.assertTrue(txn1.current() is txn2)
                    self.assertTrue(txn2.current() is txn2)
                self.assertTrue(txn1.current() is txn1)
                self.assertTrue(txn2.current() is txn1)
            self.assertTrue(txn1.current() is None)
            self.assertTrue(txn2.current() is None)

    def test_txn_begin(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.begin_transaction.return_value = 234
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SERIALIZABLE)
            self.assertTrue(mock.called)
            txn.begin()
            self.assertEqual(txn.id, 234)

    def test_txn_begin_none(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.begin_transaction.return_value = 234
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.NONE)
            txn.begin()
            self.assertEqual(txn.id, None)

    def test_txn_begin_tombstoned(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.begin_transaction.return_value = 234
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SNAPSHOT)
            self.assertTrue(mock.called)
            txn.begin()
            self.assertEqual(txn.id, 234)

            txn.rollback()
            self.assertEqual(txn.id, None)

            self.assertRaises(ValueError, txn.begin)

    def test_txn_rollback(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.begin_transaction.return_value = 234
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SERIALIZABLE)
            self.assertTrue(mock.called)
            txn.begin()
            txn.rollback()
            self.assertEqual(txn.id, None)
            self.assertEqual(txn._status, Transaction._ABORTED)

    def test_txn_commit_no_auto_ids(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.begin_transaction.return_value = 234
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SNAPSHOT)
            self.assertTrue(mock.called)
            txn.begin()
            txn.commit()
            self.assertEqual(txn._status, Transaction._FINISHED)
            self.assertEqual(txn.id, None)

    def test_txn_commit_w_auto_ids(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        with patch('gcloudoem.properties.get_connection', return_value=connection):
            resp = datastore_pb.CommitResponse()
            resp.mutation_result.insert_auto_id_key.extend([self._make_key_pb()])
        connection.begin_transaction.return_value = 234
        connection.commit.return_value = resp.mutation_result
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SNAPSHOT)
            self.assertTrue(mock.called)
            entity = self.TestEntity()
            txn._add_auto_id_entity(entity)
            self.assertEqual(len(txn._auto_id_entities), 1)
            txn.begin()
            txn.commit()
            self.assertEqual(txn._status, txn._FINISHED)
            self.assertEqual(txn.id, None)
            self.assertEqual(entity.key.id, 1234)

    def test_context_manager_no_raise(self):
        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.begin_transaction.return_value = 234
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SNAPSHOT)
            self.assertTrue(mock.called)
            with txn:
                self.assertEqual(txn.id, 234)
                self.assertEqual(txn._status, Transaction._IN_PROGRESS)
            self.assertEqual(txn._status, Transaction._FINISHED)
            self.assertEqual(txn.id, None)

    def test_context_manager_w_raise(self):
        class Foo(Exception):
            pass

        connection = MagicMock(spec=Connection)
        connection.dataset = 'DATASET'
        connection.begin_transaction.return_value = 234
        with patch('gcloudoem.datastore.transaction.get_connection', return_value=connection) as mock:
            txn = Transaction(Transaction.SNAPSHOT)
            self.assertTrue(mock.called)
            try:
                with txn:
                    self.assertEqual(txn.id, 234)
                    self.assertEqual(txn._status, Transaction._IN_PROGRESS)
                    raise Foo()
            except Foo:
                self.assertEqual(txn.id, None)
                self.assertEqual(txn._status, Transaction._ABORTED)
            self.assertEqual(txn.id, None)
