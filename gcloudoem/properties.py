# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import

import calendar
import datetime
import json
import pickle
import pytz
import six
import zlib

from google.protobuf.internal.type_checkers import Int64ValueChecker

from .base.properties import BaseProperty
from .datastore.connection import get_connection
from .key import Key


INT_VALUE_CHECKER = Int64ValueChecker()


class BooleanProperty(BaseProperty):
    """A bool property."""
    def from_protobuf(self, pb_value):
        return pb_value.boolean_value

    def to_protobuf(self, value):
        return 'boolean_value', value

    def validate(self, value):
        if not isinstance(value, bool):
            self.error('Value must be a boolean')
        return value


class KeyProperty(BaseProperty):
    """
    The Key for an Entity.

    If this property has no value, an automatic int value will be generated for it and assigned as it's id component.

    This class shouldn't be used directly. An instance of this class is added to the `key` attribute of each
    :class:`~gcloud.base.entity.BaseEntity` instance automatically. You can either let an automatic value be assigned to
    that key or assign a value manually as follows:

        >>> e = Entity(key=1)  # e has a KeyProperty at .key with an id component of 1
        >>> e = Entity(key='1')  # e has a KeyProperty at .key with an name component of '1'
        >>> e = Entity()  # e will have a KeyProperty at .key with a generated id component when .save() is called
        >>> e.key = 1  # e now has a KeyProperty at .key with an id component of 1

    In Datastore, all keys must have a kind. The kind is automatically set the the ``__class__`` attribute of the owning
    :class:`~gcloud.base.entity.BaseEntity` subclass. The value for this property is a dict containing the kind and the
    id (int) or name (str in Python 3, unicode in Python 2). For example:

        >>> e = Entity(key=1)
        >>> e.key
        {'kind': 'Entity', 'id': 1}
        >>> e = Entity(key='1')
        >>> e.key
        {'kind': 'Entity', 'name': 1}
        >>> e = Entity()
        >>> e.key
        {'kind': 'Entity'}

    As you can see, when a key has an auto generated id, the id component will remain empty until the entity is saved.

    Even though the value of a key is a dict, you must pass either an int or str (unicode in Python 2) to set its value:

        >>> e = Entity()
        >>> e.key
        {'kind': 'Entity'}
        >>> e.key = 1
        >>> e.key
        {'kind': 'Entity', 'id': 1}
    """
    def __init__(self, name=None, db_name=None):
        super(KeyProperty, self).__init__(name=name, db_name=db_name)

    def __get__(self, instance, owner):
        if not instance:
            return self
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        kind = instance._meta.kind
        parent = None

        # Is this a (parent, <value>) tuple?
        if isinstance(value, tuple):
            parent = value[0]
            if len(value) == 2:
                value = value[1]  # Actual key value as 2nd element
            else:
                value = None  # Partial key

        if isinstance(value, Key):
            instance._data[self.name] = value
        else:
            instance._data[self.name] = Key(kind, parent=parent, value=value)

    def to_protobuf(self, value):
        from .datastore import datastore_v1_pb2 as datastore_pb
        key = datastore_pb.Key()

        dataset_id = get_connection().dataset
        if not dataset_id:
            raise EnvironmentError("Couldn't determine the dataset ID. Have you called connect?")
        key.partition_id.dataset_id = dataset_id

        for item in value.path:
            element = key.path_element.add()
            element.kind = item.kind
            if item.id:
                element.id = item.id
            elif item.name:
                element.name = item.name

        return key

    def from_protobuf(self, pb_value):
        """
        Factory method for creating a key based on a protobuf.

        The protobuf should be one returned from the Datastore protobuf API.

        :type pb_value: :class:`gcloudoem.datastore.datastore_v1_pb2.Key`
        :param pb_value: The Protobuf representing the key.

        :rtype: :class:`gcloudoem.key.Key`
        :returns: a new `Key` instance
        """
        last_key = None
        for element in pb_value.path_element:
            if element.HasField('id'):
                key = Key(element.kind, parent=last_key, value=element.id)
            elif element.HasField('name'):
                key = Key(element.kind, parent=last_key, value=element.name)
            else:
                key = Key(element.kind, parent=last_key)
            last_key = key

        return last_key


class IntegerProperty(BaseProperty):
    """An int property."""
    def from_protobuf(self, pb_value):
        return pb_value.integer_value

    def to_protobuf(self, value):
        INT_VALUE_CHECKER.CheckValue(value)
        return 'integer_value', int(value)

    def validate(self, value):
        if not isinstance(value, six.integer_types):
            self.error('Value must be an int (or long in Python 2).')
        return int(value)


class FloatProperty(BaseProperty):
    """A float property."""
    def from_protobuf(self, pb_value):
        return pb_value.double_value

    def to_protobuf(self, value):
        return 'double_value', value

    def validate(self, value):
        if not isinstance(value, six.integer_types + (float,)):
            self.error('Value must be an int or float (or long in Python 2).')
        return float(value)


class BlobProperty(BaseProperty):
    """Store data as bytes. Supports compression."""
    def __init__(self, compressed=False, **kwargs):
        """
        Initialise this property. Has an option to compress using zlib that defaults to False. **Note** that this
        property can't be compressed and indexed!

        :param bool compressed: should this property store its value compressed? Defaults to False.
        """
        super(BlobProperty, self).__init__(**kwargs)

        self._compressed = compressed

    def from_protobuf(self, pb_value):
        value = pb_value.blobl_value
        if self._compressed:
            return zlib.decompress(value)
        return value

    def to_protobuf(self, value):
        if self._compressed:
            return zlib.compress(value)
        return 'blob_value', value

    def validate(self, value):
        if not isinstance(value, six.binary_type):
            self.error("Value must be bytes (or str in Python 2).")
        return value


class TextProperty(BlobProperty):
    """Store data as unicode."""
    def from_protobuf(self, pb_value):
        value = pb_value.string_value
        if isinstance(value, six.binary_type):
            return value.encode('utf-8')
        return value

    def to_protobuf(self, value):
        if isinstance(value, six.binary_type):
            return value.decode('utf-8')
        return 'string_value', value

    def validate(self, value):
        if not isinstance(value, six.text_type):
            self.error('Value must be str (unicode in Python 2)')
        return value


class PickleProperty(BlobProperty):
    """Store data as pickle. Takes care of (un)pickling."""
    def to_datastore_value(self, value):
        return super(PickleProperty, self).to_datastore_value(pickle.dumps(value, pickle.HIGHEST_PROTOCOL))

    def to_python_value(self, value):
        return pickle.loads(super(PickleProperty, self)._from_base_type(value))


class JsonProperty(BlobProperty):
    """Store data as JSON. Takes care of conversion to/from JSON."""
    def __init__(self, name=None, schema=None, **kwargs):
        super(JsonProperty, self).__init__(name, **kwargs)
        self._schema = schema

    def to_datastore_value(self, value):
        return super(JsonProperty, self)._to_base_type(json.dumps(value))

    def to_python_value(self, value):
        return json.loads(super(JsonProperty, self)._from_base_type(value))


class DateTimeProperty(BaseProperty):
    """Store data as a timestamp represented as datetime.datetime."""
    def __init__(self, name=None, auto_now_add=False, auto_now=False, **kwargs):
        assert not ((auto_now_add or auto_now) and kwargs.get("repeated", False))
        super(DateTimeProperty, self).__init__(name, **kwargs)
        self._auto_now_add = auto_now_add
        self._auto_now = auto_now

    def from_protobuf(self, pb_value):
        microseconds = pb_value.timestamp_microseconds_value
        naive = (datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(microseconds=microseconds))
        return naive.replace(tzinfo=pytz.utc)

    def to_protobuf(self, value):
        name = 'timestamp_microseconds_value'
        # If the datetime is naive (no timezone), consider that it was
        # intended to be UTC and replace the tzinfo to that effect.
        if not value.tzinfo:
            value = value.replace(tzinfo=pytz.utc)
        # Regardless of what timezone is on the value, convert it to UTC.
        value = value.astimezone(pytz.utc)
        # Convert the datetime to a microsecond timestamp.
        value = int(calendar.timegm(value.timetuple()) * 1e6) + value.microsecond
        return name, value

    def validate(self, value):
        if not isinstance(value, datetime.datetime):
            self.error('Value must be a datetime.datetime')
        return value

    def _now(self):
        return datetime.datetime.utcnow()


class DateProperty(DateTimeProperty):
    """Store data as a date and represented as datetime.date."""
    def validate(self, value):
        if not isinstance(value, datetime.date):
            self.error('Value must be a datetime.date')
        return value

    def to_datastore_value(self, value):
        return datetime.datetime(value.year, value.month, value.day)

    def to_python_value(self, value):
        return value.date()

    def _now(self):
        return datetime.datetime.utcnow().date()


class TimeProperty(DateTimeProperty):
    """Store data as time represented using datetime.time."""
    def _validate(self, value):
        if not isinstance(value, datetime.time):
            self.error("Value must be a datetime.time")
        return value

    def to_datastore_value(self, value):
        return datetime.datetime(
            1970, 1, 1,
            value.hour, value.minute, value.second,
            value.microsecond
        )

    def to_python_value(self, value):
        return value.time()


class ListProperty(BaseProperty):
    """
    A property that supports a list of properties.

    Only supports one type of property at a time.

    .. note::
        Required means it cannot be empty - as the default for ListProperty is []
    """
    def __init__(self, property, **kwargs):
        """
        :param :class:`~gcloudoem.base.properties.BaseProperty` property: The property class used as the value of the
            ``list`` items. Can't be a :class:`~gcloudoem.properties.KeyProperty`
        """
        if not issubclass(property, BaseProperty):
            raise TypeError('property must be a BaseProperty Instance')
        if issubclass(property, (KeyProperty, ListProperty)):
            raise TypeError('property cannot be a KeyProperty or ListProperty')
        self.property = property
        kwargs.pop('default', lambda: [])
        super(ListProperty, self).__init__(**kwargs)

    def from_protobuf(self, pb_value):
        return [self.property.from_protobuf(v) for v in pb_value.list_value]

    def to_protobuf(self, value):
        return 'list_value', [self.property.to_protobuf(v) for v in value]

    def validate(self, value):
        if not isinstance(value, (list, tuple)):
            self.error('Value must be a list or tuple')
        for item in value:
            if not isinstance(item, type(self.property)):
                self.error('All values in the list must be a %s instance' % type(self.property))
