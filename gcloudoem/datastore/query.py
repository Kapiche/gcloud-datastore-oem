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
"""Create / interact with gcloud datastore queries."""
from __future__ import absolute_import, division, print_function

import base64

import six

from . import datastore_v1_pb2 as datastore_pb, utils
from .connection import get_connection
from ..exceptions import InvalidQueryError
from ..key import Key
from .transaction import Transaction


class Query(object):
    """
    A Google Datastore Query.

    This class serves as an abstraction for creating a query over data stored in Datastore.
    """

    OPERATORS = {
        '<=': datastore_pb.PropertyFilter.LESS_THAN_OR_EQUAL,
        'lte': datastore_pb.PropertyFilter.LESS_THAN_OR_EQUAL,
        '>=': datastore_pb.PropertyFilter.GREATER_THAN_OR_EQUAL,
        'gte': datastore_pb.PropertyFilter.GREATER_THAN_OR_EQUAL,
        '<': datastore_pb.PropertyFilter.LESS_THAN,
        'lt': datastore_pb.PropertyFilter.LESS_THAN,
        '>': datastore_pb.PropertyFilter.GREATER_THAN,
        'gt': datastore_pb.PropertyFilter.GREATER_THAN,
        '=': datastore_pb.PropertyFilter.EQUAL,
        'eq': datastore_pb.PropertyFilter.EQUAL,
    }
    """Mapping of operator strs and their protobuf equivalents."""

    def __init__(self, entity, ancestor=None, filters=(), projection=(), order=(), group_by=(),
                 limit=None, offset=0):
        """
        Initialise a new Query.

        :type entity: type
        :param entity: The entity class to use for the query. Used to derive the ``kind`` passed to datastore.

        :type ancestor: :class:`~gcloudoem.entity.Entity` or None
        :param ancestor: the ancestor to which this query's results are restricted.

        :type filters: sequence of (property_name, operator, value) tuples
        :param filters: property filters applied by this query.

        :type projection: sequence of str
        :param projection: fields to be returned as part of query results. An empty sequence means all fields.

        :type order: sequence of str
        :param order: field names used to order query results. Prepend '-' to a field name to sort it in descending
            order.

        :type group_by: sequence of str
        :param group_by: field names used to group query results.

        :type limit: int
        :param limit: number of entity results to limit this query to. None means don't limit. Defaults to None.

        :type offset: int
        :param offset: the offset into the results the first entity should be. Defaults to 0.
        """
        from gcloudoem import Entity

        if not isinstance(entity, type) or not issubclass(entity, Entity):
            raise ValueError('You must pass a valid entity class to query (one that subclasses Entity)')

        self._entity = entity
        self._ancestor = ancestor
        self._filters = list()
        self._projection = list(projection)
        self._order = list(order)
        self._group_by = list(group_by)
        self._limit = limit
        self._offset = offset
        self._has_inequality_filter = None

        for f in filters:
            self.add_filter(*f)

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, limit):
        self._limit = limit

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, offset):
        self._offset = offset

    def set_limits(self, offset, limit):
        """Shortcut to set the offset and the limit. Useful for slices."""
        self._offset = offset
        self._limit = limit

    def is_limited(self):
        """Has an offset or limit been applied to this query?"""
        return bool(self._offset or self._limit)

    @property
    def entity(self):
        """
        Get the Kind of the Query.

        :rtype: str
        """
        return self._entity

    @property
    def ancestor(self):
        """
        The ancestor key for the query.

        :rtype: :class:`~gcloudoem.entity.Entity` or None
        """
        return self._ancestor

    @property
    def filters(self):
        """
        Filters set on the query.

        :rtype: sequence of (property_name, operator, value) tuples.
        """
        return self._filters[:]

    def add_filter(self, property_name, operator, value):
        """
        Add a filter to the query based on a property name, operator and a value.

        Expressions take the form of::

          .add_filter('<property>', '<operator>', <value>)

        where property is the name of a property stored on the entity for this query, operator is one of ``OPERATORS``
        (ie, ``=``, ``<``, ``<=``, ``>``, ``>=``) and value is the value to filter on::

            >>> from gcloudoem import entity, properties
            >>> from gcloudoem.datastore.query import Query
            >>> class Person(entity.Entity):
            ...     name = properties.TextProperty()
            ...     age = properties.IntegerProperty()
            ...
            >>> query = Query(Person)
            >>> query.add_filter('name', '=', 'James')
            >>> query.add_filter('age', '>', 50)

        :type property_name: str
        :param property_name: A property name. Used to fetch the corresponding property off the Entity for this query.

        :type operator: str
        :param operator: One of ``=``, ``<``, ``<=``, ``>``, ``>=``. See :attr:`Query.OPERATORS`.

        :type value: int, str, bool, float, None, datetime
        :param value: The value to filter on.

        :raises: :class:`gcloudoem.exceptions.InvalidQueryError` if:
            * `operation` is not one of the specified values, or
            * query.entity doesn't have a property by the name ``property_name``, or
            * filter names `'__key__'` but passes an invalid operator (``=`` is required) or value (a Key is required).
        """
        if self.OPERATORS.get(operator) is None:
            error_message = 'Invalid expression: "%s"' % (operator,)
            choices_message = 'Please use one of: =, <, <=, >, >=.'
            raise InvalidQueryError(error_message, choices_message)

        if property_name == 'key':
            if not isinstance(value, (Key,) + six.string_types + six.integer_types):
                raise InvalidQueryError('Invalid key value "%s"' % type(value))
            if not isinstance(value, Key):
                value = Key(self._entity._meta.kind, value=value)
            if self.OPERATORS[operator] != datastore_pb.PropertyFilter.EQUAL:
                raise InvalidQueryError('Invalid operator for key: "%s"' % operator)
        elif not hasattr(self._entity, property_name):
            raise InvalidQueryError("Entity %s used in this Query doesn't have a property %s" %
                                    (self._entity._meta.kind, property_name))

        if self.OPERATORS[operator] in (
            datastore_pb.PropertyFilter.LESS_THAN_OR_EQUAL, datastore_pb.PropertyFilter.GREATER_THAN_OR_EQUAL,
        ):
            if not self._has_inequality_filter:
                self._has_inequality_filter = property_name
            elif property_name != self._has_inequality_filter:
                raise InvalidQueryError(
                    "Datastore only supports inequality operators on a single property within a query."
                )

        self._filters.append((property_name, operator, value))

    @property
    def projection(self):
        """
        Fields names returned by the query.

        :rtype: sequence of str
        :returns: Names of fields in query results.
        """
        return self._projection[:]

    @projection.setter
    def projection(self, value):
        """
        :raises: :class:`gcloudoem.exceptions.InvalidQueryError` if the property name(s) in value don't exist on the
            entity for this query.
        """
        if isinstance(value, str):
            value = [value]
        for projection in value:
            if not projection == '__key__' and not hasattr(self._entity, projection):
                raise InvalidQueryError("Entity %s used in this Query doesn't have a property %s" %
                                        (self.entity._meta.kind, projection))
        self._projection[:] = value

    def keys_only(self):
        """Set the projection to include only keys."""
        self._projection[:] = ['__key__']

    @property
    def order(self):
        """
        Names of fields used to sort query results.

        :rtype: sequence of str
        """
        return self._order[:]

    @order.setter
    def order(self, value):
        """
        Set the fields used to sort query results.

        Sort fields will be applied in the order specified.

        :type value: str or sequence of str
        :param value: Each value is a str giving the name of the property on which to sort, optionally preceded by a
            hyphen (-) to specify descending order. Omitting the hyphen implies ascending order.

        :raises: :class:`gcloudoem.exceptions.InvalidQueryError` if the property name(s) in value don't exist on the
            entity for this query.
        """
        if isinstance(value, str):
            value = [value]
        for prop_name in value:
            property = prop_name[1:] if prop_name[0] == '-' else prop_name
            if not hasattr(self._entity, property):
                raise InvalidQueryError("Entity %s used in this Query doesn't have a property %s" %
                                        (self._entity._meta.kind, prop_name))
        self._order[:] = value

    @property
    def group_by(self):
        """
        Names of fields used to group query results.

        :rtype: sequence of str
        """
        return self._group_by[:]

    @group_by.setter
    def group_by(self, value):
        """
        Set fields used to group query results.

        :type value: str or sequence of strs
        :param value: Each value is a str giving the name of a property to use to group results together.

        :raises: :class:`gcloudoem.exceptions.InvalidQueryError` if the property name(s) in value don't exist on the
            entity for this query.
        """
        if isinstance(value, str):
            value = [value]
        for prop_name in value:
            if not hasattr(self._entity, prop_name):
                raise InvalidQueryError("Entity %s used in this Query doesn't have a property %s" %
                                        (self._entity._meta.kind, prop_name))
        self._group_by[:] = value

    def __call__(self):
        """
        Execute the Query; return a :class:`Cursor` for the matching entities.

        For example::

            >>> from gcloudoem.datastore.query import Query
            >>> query = Query('Person')
            >>> query.add_filter('name', '=', 'Sally')
            >>> list(query.execute())
            [<Entity object>, <Entity object>, ...]
            >>> query.limit = 1
            >>> list(query.execute(1))
            [<Entity object>]

        For an explication of the options, see
        https://cloud.google.com/datastore/docs/concepts/queries#Datastore_Query_cursors.

        :rtype: :class:`Cursor`
        :raises: :class:`~gcloudoem.exceptions.ConnectionError` if there is no active connection.
        """
        connection = get_connection()

        return Cursor(self, connection, self.limit, self.offset)

    def clone(self):
        return self.__class__(
            self._entity, self._ancestor, self.filters, self.projection, self.order, self.group_by,
            self._limit, self._offset
        )

    def to_protobuf(self):
        """
        Convert this Query instance to the corresponding protobuf representation.

        :rtype: :class:`~gcloudoem.datastore.datastore_v1_pb2.Query`
        :returns: A protobuf query that can be sent to the protobuf API.  N.b. that it does not contain "in-flight"
            fields for ongoing query executions (cursors, offset, limit).
        """
        pb = datastore_pb.Query()

        for projection_name in self._projection:
            pb.projection.add().property.name = projection_name

        if self._entity:
            pb.kind.add().name = self._entity._meta.kind

        composite_filter = pb.filter.composite_filter
        composite_filter.operator = datastore_pb.CompositeFilter.AND

        if self.ancestor:
            ancestor_pb = utils.prepare_key_for_request(self.ancestor._properties['key'].to_protobuf(self.ancestor.key))

            # Filter on __key__ HAS_ANCESTOR == ancestor.
            ancestor_filter = composite_filter.filter.add().property_filter
            ancestor_filter.property.name = '__key__'
            ancestor_filter.operator = datastore_pb.PropertyFilter.HAS_ANCESTOR
            ancestor_filter.value.key_value.CopyFrom(ancestor_pb)

        for property_name, operator, value in self.filters:
            pb_op_enum = self.OPERATORS.get(operator)

            # Add the specific filter
            prop = self._entity._properties[property_name]
            property_filter = composite_filter.filter.add().property_filter
            property_filter.property.name = prop.db_name
            property_filter.operator = pb_op_enum

            # Set the value to filter on based on the type.
            if property_name == 'key':
                key_pb = prop.to_protobuf(value)
                property_filter.value.key_value.CopyFrom(utils.prepare_key_for_request(key_pb))
            else:
                attr, pb_value = prop.to_protobuf(value)
                utils.set_protobuf_value(property_filter.value, attr, pb_value)

        if not composite_filter.filter:
            pb.ClearField('filter')

        for prop in self.order:
            property_order = pb.order.add()

            if prop.startswith('-'):
                property_order.property.name = prop[1:]
                property_order.direction = property_order.DESCENDING
            else:
                property_order.property.name = prop
                property_order.direction = property_order.ASCENDING

        for group_by_name in self.group_by:
            pb.group_by.add().name = group_by_name

        return pb


class Cursor(object):
    """
    Represent the state of a given execution of a Query.

    This class is a generator that can be iterated.
    """

    _NOT_FINISHED = datastore_pb.QueryResultBatch.NOT_FINISHED

    _FINISHED = (
        datastore_pb.QueryResultBatch.NO_MORE_RESULTS,
        datastore_pb.QueryResultBatch.MORE_RESULTS_AFTER_LIMIT,
    )

    def __init__(self, query, connection, limit=None, offset=0, start_cursor=None, end_cursor=None):
        self._query = query
        self._connection = connection
        self._limit = limit
        self._offset = offset
        self._start_cursor = start_cursor
        self._end_cursor = end_cursor
        self._page = self._more_results = None

    def next_page(self):
        """
        Fetch a single "page" of query results.

        Low-level API for fine control: the more convenient API is to iterate on the current Iterator.

        :rtype: tuple, (entities, more_results, cursor)
        """
        pb = self._query.to_protobuf()

        start_cursor = self._start_cursor
        if start_cursor is not None:
            pb.start_cursor = base64.b64decode(start_cursor)

        end_cursor = self._end_cursor
        if end_cursor is not None:
            pb.end_cursor = base64.b64decode(end_cursor)

        if self._limit is not None:
            pb.limit = self._limit

        pb.offset = self._offset

        transaction = Transaction.current()

        query_results = self._connection.run_query(
            query_pb=pb,
            namespace=self._connection.namespace,
            transaction_id=transaction and transaction.id,
        )
        # NOTE: `query_results` contains an extra value that we don't use, namely `skipped_results`.
        #
        # NOTE: The value of `more_results` is not currently useful because the back-end always returns an enum value of
        #       MORE_RESULTS_AFTER_LIMIT even if there are no more results. See
        #       https://github.com/GoogleCloudPlatform/gcloud-python/issues/280 for discussion.
        entity_pbs, cursor_as_bytes, more_results_enum = query_results[:3]

        self._start_cursor = base64.b64encode(cursor_as_bytes)
        self._end_cursor = None

        if more_results_enum == self._NOT_FINISHED:
            self._more_results = True
        elif more_results_enum in self._FINISHED:
            self._more_results = False
        else:
            raise RuntimeError('Unexpected value returned for `more_results`.')

        self._page = [self._query.entity.from_protobuf(pb) for pb in entity_pbs]
        return self._page, self._more_results, self._start_cursor

    def __iter__(self):
        """
        Generator yielding all results matching our query.

        :rtype: sequence of :class:`gcloud.datastore.entity.Entity`
        """
        self.next_page()
        while True:
            for entity in self._page:
                yield entity
            if not self._more_results:
                break
            self.next_page()
