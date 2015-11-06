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
# Copyright 2015 Kapiche Ltd. All rights reserved.
# Based on work by the good folk responsible for gcloud-python. Thanks folks!
# Author: Ryan Stuart<ryan@kapiche.com>
#
"""Create / interact with a datastore transaction."""
from __future__ import absolute_import, division, print_function, unicode_literals

from . import datastore_v1_pb2 as datastore_pb, environment, utils
from .connection import get_connection
from ..properties import KeyProperty, ListProperty, ReferenceProperty

_TRANSACTIONS = utils._LocalStack()


class Transaction(object):
    """
    An abstraction representing a collected group of updates / deletes as a datastore transaction.

    Used to build up a bulk mutation.

    For example, the following snippet of code will put the two ``save`` operations and the delete operation into the
    same mutation, and send them to the server in a single API request::

      >>> from gcloudoem.datastore.transaction import Transaction
      >>> transaction = Transaction()
      >>> transaction.put(entity1)
      >>> transaction.put(entity2)
      >>> transaction.delete(entity3)
      >>> transaction.commit()

    You can also use a transaction as a context manager, in which case the ``commit`` will be called automatically if
    its block exits without raising an exception::

      >>> with Transaction() as transaction:
      ...     transaction.put(entity1)
      ...     transaction.put(entity2)
      ...     transaction.delete(key3)

    By default, no updates will be sent if the block exits with an error::

      >>> with Transaction() as transaction:
      ...   do_some_work(batch)
      ...   raise Exception() # rolls back

    After completion, you can determine if a commit succeeded or failed. For example, trying to delete a key that
    doesn't exist::

         >>> with Transaction(Transaction.SNAPSHOT) as xact:
         ...     xact.delete(entity)
         ...
         >>> xact.succeeded
         False

    If you don't want to use the context manager you can initialize a transaction manually::

      >>> transaction = Transaction()
      >>> transaction.begin()
      >>> entity = MyEntity(key=123)
      >>> transaction.put(entity)
      >>> if error:
      ...     transaction.rollback()
      ... else:
      ...     transaction.commit()

    Transactions have 3 isolation levels. They are (in order of strictness):

    #. NONE - No isolation. Everything is just wrapped in a single mutation and sent to datastore.
    #. SNAPSHOT - another transaction may not concurrently modify the data modified by this transaction.
    #. SERIALIZABLE - another transaction cannot concurrently modify the data that is read or modified by this
                      transaction.

    See https://cloud.google.com/appengine/articles/transaction_isolation for more information
    """

    NONE = None
    """No transaction isolation."""

    SNAPSHOT = False
    """Snapshot transaction isolation."""

    SERIALIZABLE = True
    """Serializable transaction isolation."""

    # Transaction states
    _INITIAL = 0
    _IN_PROGRESS = 1
    _ABORTED = 2
    _FINISHED = 3

    def __init__(self, isolation):
        """
        Construct a transaction.

        :type isolation: :class:`bool` or None. Use the class attributes as shortcuts.
        :param dataset_id: Transaction isolation level. None = NONE, False = SNAPSHOT and True = SERIALIZABLE.

        :raises: :class:`~gcloudoem.exceptions.ConnectionError` if there is no active connection.
        """
        self._connection = get_connection()
        self._mutation = datastore_pb.Mutation()
        self._auto_id_entities = []
        self._id = None
        self._status = self._INITIAL
        self._isolation = isolation

    @staticmethod
    def current():
        """Return the topmost batch / transaction, or None."""
        return _TRANSACTIONS.top

    @property
    def id(self):
        """
        Getter for the transaction ID.

        :rtype: str
        :returns: The ID of the current transaction.
        """
        return self._id

    def _add_auto_id_entity(self, entity):
        """
        Adds an entity to the list of entities to update with IDs.

        When an entity has a partial key, calling ``save()`` adds an insert_auto_id entry in the mutation.  In order to
        make sure we update the Entity once the transaction is committed, we need to keep track of which entities to
        update (and the order is important).

        When you call ``save()`` on an entity inside a transaction, if the entity has a partial key, it adds itself to
        the list of entities to be updated once the transaction is committed by calling this method.

        :type entity: :class:`~gcloudoem.entity.Entity`
        :param entity: The entity to be updated with a completed key.

        :raises: ValueError if the entity's key is already completed.
        """
        if not entity.key.is_partial:
            raise ValueError("Entity has a completed key")

        self._auto_id_entities.append(entity)

    def _assign_entity_to_mutation(self, entity, force_insert=False):
        """
        Copy ``entity`` into appropriate slot of the mutation for this transaction.

        If ``entity.key`` is incomplete, append ``entity`` to self.auto_id_entities for later fixup during ``commit``.

        :type entity: :class:`~gcloudoem.entity.Entity`
        :param entity; the entity being updated within the batch / transaction.

        :type force_insert: bool
        :param force_insert: Assign this entity to the insert mutation instead of upsert. Defaults to False.
        """
        # Prepare the key
        key = getattr(entity, 'key')
        key_pb = entity._properties['key'].to_protobuf(key)
        key_pb = utils.prepare_key_for_request(key_pb)

        # What type of mutation is this?
        if key.is_partial:  # autogen key?
            insert = self._mutation.insert_auto_id.add()
            self._auto_id_entities.append(entity)
        elif force_insert:
            insert = self._mutation.insert.add()
        else:
            insert = self._mutation.upsert.add()  # The default. Update or insert.

        # Add the key
        insert.key.CopyFrom(key_pb)

        for name, property in entity._properties.items():
            if isinstance(property, KeyProperty):  # Already dealt with the key
                continue

            value_is_reference = isinstance(property, ReferenceProperty)
            if value_is_reference:
                value = entity._data[property.name]  # We are happy with the Key, don't fetch the entity
            else:
                value = getattr(entity, name)
                if value is None:  # Nothing to save.
                    continue

            value_is_list = isinstance(property, ListProperty)
            if value_is_list and len(value) == 0:  # Nothing to save
                continue

            # Create the property
            prop = insert.property.add()
            prop.name = property.db_name
            attr, value = property.to_protobuf(value)
            utils.set_protobuf_value(prop.value, attr, value)

            if property.exclude_from_index:
                if not value_is_list:
                    prop.value.indexed = False

                for sub_value in prop.value.list_value:
                    sub_value.indexed = False

    def put(self, entity):
        """
        Store entity as part of this transaction.

        Nothing happens until :meth:`commit` is called, which happens automatically when this class is used as a context
        manager (ie using the with statement).

        .. note::
           Any existing properties for the entity will be replaced by those currently set on this instance.
           Already-stored  properties which do not correspond to keys set on this instance will be removed from the
           datastore.

        .. note::
           Property values which are "text" ('unicode' in Python2, 'str' in Python3) map to 'string_value' in the
           datastore; values which are "bytes" ('str' in Python2, 'bytes' in Python3) map to 'blob_value'.

        :type entity: :class:`~gcloud.entity.Entity`
        :param entity: the entity to be saved.
        """
        if entity.key is None:
            raise ValueError("Entity must have a key")

        self._assign_entity_to_mutation(entity)

    def create(self, entity):
        """
        Store entity as part of this transaction.

        This method is identical to :meth:`put` except that it it uses the ``insert`` slot of the Datastore mutation
        rather then ``upsert``.

        :type entity: :class:`~gcloud.entity.Entity`
        :param entity: the entity to be saved.

        :raises: ValueError if entity has no key assigned
        """
        if entity.key is None:
            raise ValueError("Entity must have a key")

        self._assign_entity_to_mutation(entity, force_insert=True)

    def delete(self, entity):
        """
        Delete entity during this transaction.

        Nothing happens until :meth:`commit` is called, which happens automatically when this class is used as a context
        manager (ie using the with statement).

        :type entity: :class:`~gcloudoem.entity.Entity`
        :param entity: the entity to be deleted.

        :raises: ValueError if key is not complete, or if the key's
                 ``dataset_id`` does not match ours.
        """
        if entity.key.is_partial:
            raise ValueError("Entity myst have a complete key")

        key_pb = utils.prepare_key_for_request(entity._properties['key'].to_protobuf(entity.key))
        self._mutation.delete.add().CopyFrom(key_pb)

    def begin(self):
        """
        Begin this transaction.

        If the isolation level is :attr:`NONE`, this is basically a no-op.

        :raises: :class:`ValueError` if the transaction has already begun.
        """
        if self._isolation == Transaction.NONE:
            return

        if self._status != self._INITIAL:
            raise ValueError('Transaction already started previously.')
        self._status = self._IN_PROGRESS
        self._id = self._connection.begin_transaction(serializable=self._isolation)

    def commit(self):
        """
        Commits the batch.

        This is called automatically upon exiting a with statement, however it can be called explicitly if you don't
        want to use a context manager.
        """
        try:
            response = self._connection.commit(self._mutation, self._id)
            # If the back-end returns without error, we are guaranteed that the response's 'insert_auto_id_key' will
            # match (length and order) the request's 'insert_auto_id` entities, which are derived from our
            # '_auto_id_entities' (no partial success).
            for new_key_pb, entity in zip(response.insert_auto_id_key, self._auto_id_entities):
                entity._data['key']._id = new_key_pb.path_element[-1].id
        finally:
            self._status = self._FINISHED
            # Clear our own ID in case this gets accidentally reused.
            self._id = None

    def rollback(self):
        """
        Rollback the transaction.

        This is a no-op for the :attr:`NONE` isolation level.

        Otherwise, this method has necessary side-effects:

        * Sets the current connection's transaction reference to None.
        * Sets the current transaction's ID to None.
        """
        try:
            if self._isolation != Transaction.NONE:
                self._connection.rollback(self._id)
        finally:
            self._status = self._ABORTED
            # Clear our own ID in case this gets accidentally reused.
            self._id = None

    def __enter__(self):
        _TRANSACTIONS.push(self)
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            _TRANSACTIONS.pop()
