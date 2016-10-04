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
"""
until functions for dealing with Cloud Datastore's Protobuf API.

The non-private functions are part of the API.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

from threading import local

from ._generated import datastore_pb2 as datastore_pb


__all__ = ('set_protobuf_value', 'prepare_key_for_request')


class _LocalStack(local):
    """
    Manage a thread-local LIFO stack of resources.
    Intended for use in :class:`gcloudoem.datastore.transaction.Transaction.__enter__`,
    :class:`~gcloudoem.datastore.transaction.Transaction.__enter__`, etc.
    """
    def __init__(self):
        super(_LocalStack, self).__init__()
        self._stack = []

    def __iter__(self):
        """Iterate the stack in LIFO order."""
        return iter(reversed(self._stack))

    def push(self, resource):
        """Push a resource onto our stack."""
        self._stack.append(resource)

    def pop(self):
        """
        Pop a resource from our stack.

        :raises: IndexError if the stack is empty.
        :returns: the top-most resource, after removing it.
        """
        return self._stack.pop()

    @property
    def top(self):
        """
        Get the top-most resource

        :returns: the top-most item, or None if the stack is empty.
        """
        if len(self._stack) > 0:
            return self._stack[-1]


def set_protobuf_value(protobuf_obj, attr, pb_value):
    """
    Assign ``pb_value`` the correct subfield of ``protobuf_obj`` based on ``attr``.

    The Protobuf API uses different attribute names based on value types rather than inferring the type.

    Some value types (keys, lists) cannot be directly assigned; this function handles them correctly. In particular, it
    will call itself recursively for for list values.

    :type protobuf_obj: :class:`gcloudoem.datastore.datastore_v1_pb2.Value`
    :param protobuf_obj: The protobuf instance to which the value is being assigned.

    :type attr: str
    :param attr: The protobuf attribute name for this value. For example. 'string_value', 'array_value', etc.

    :type pb_value: `datetime.datetime`, boolean, float, integer, string
    :param pb_value: The value to be assigned in the appropriate protobuf form. Usually obtained by calling
        ``property.to_protobuf()``.
    """
    if pb_value is None:
        protobuf_obj.Clear()
        return

    if attr == 'key_value':
        protobuf_obj.key_value.CopyFrom(pb_value)
    elif attr == 'timestamp_value':
        protobuf_obj.timestamp_value.CopyFrom(pb_value)
    elif attr == 'array_value':
        l_pb = protobuf_obj.array_value.values
        for item in pb_value:
            i_pb = l_pb.add()
            set_protobuf_value(i_pb, item[0], item[1])
    else:  # scalar, just assign
        setattr(protobuf_obj, attr, pb_value)
