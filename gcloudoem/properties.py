# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import

import calendar
import datetime
import json
import pickle
import re
import pytz
import six
import zlib

from google.protobuf.internal.type_checkers import Int64ValueChecker

from .base.properties import BaseProperty, ContainerBaseProperty
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
        key.partition_id.namespace = get_connection().namespace

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


class ReferenceProperty(BaseProperty):
    """
    A reference to another entity.

    The value of this property is an Entity instance. It is required by default.
    """
    def __init__(self, entity_cls, name=None, db_name=None, required=True, verbose_name=None):
        """
        Initialise a new reference property.

        :param entity_cls: The class of the entity this property is referencing.
        """
        super(ReferenceProperty, self).__init__(name=name, db_name=db_name, required=required)
        self.entity_cls = entity_cls

    def __get__(self, instance, owner):
        """Fetch this field from the QuerySet if not already stored on the class."""
        if instance is None:
            return self
        try:
            value = instance._data[self.name]
            if isinstance(value, Key):  # We need to fetch the entity and set it on the owning entity
                value = self.entity_cls.objects.get(pk=value.name_or_id)
                setattr(instance, self.name, value)
        except KeyError:  # Empty
            pass
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        if not isinstance(value, (Key, self.entity_cls)) and value is not None:
            raise TypeError('The value of a ReferenceProperty must be an Entity or Key')
        instance._data[self.name] = value

    def from_protobuf(self, pb_value):
        data = pb_value.blob_value
        return pickle.loads(data)

    def to_protobuf(self, value):
        if isinstance(value, self.entity_cls):
            value = value.key
        return "blob_value", pickle.dumps(value, protocol=2)  # Py2 compatible

    def validate(self, value):
        if not isinstance(value, (self.entity_cls, Key)):
            self.error("A ReferenceProperty only accepts an %s instance or a Key as its value." %
                       self.entity_cls.__name__)
        if (isinstance(value, self.entity_cls) and value.key.is_partial) or \
                (isinstance(value, Key) and value.is_partial):
            self.error("You can only reference an Entity once it has been saved to Datastore.")


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
        value = pb_value.blob_value
        if self._compressed:
            return zlib.decompress(value)
        return value

    def to_protobuf(self, value):
        if self._compressed:
            value = zlib.compress(value)
        return 'blob_value', value

    def validate(self, value):
        if not isinstance(value, six.binary_type):
            self.error("Value must be bytes (or str in Python 2).")
        return value


class TextProperty(BlobProperty):
    """Store data as unicode."""
    def __init__(self, max_length=None, **kwargs):
        super(TextProperty, self).__init__(**kwargs)
        self._max_length = max_length

    @property
    def max_length(self):
        return self._max_length

    def from_protobuf(self, pb_value):
        value = pb_value.string_value
        if isinstance(value, six.binary_type):
            return value.encode('utf-8')
        return value

    def to_protobuf(self, value):
        if isinstance(value, six.binary_type):
            value = value.decode('utf-8')
        return 'string_value', value

    def validate(self, value):
        if not isinstance(value, six.string_types):
            self.error('Value must be str (unicode in Python 2)')
        if self.max_length and len(value) > self.max_length:
            self.error('Value exceeds maximum length of %d' % self.max_length)
        return value


class EmailProperty(TextProperty):
    message = 'Enter a valid email address.'
    user_regex = re.compile(
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*$"  # dot-atom
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"$)',  # quoted-string
        re.IGNORECASE
    )
    domain_regex = re.compile(
        # max length for domain name labels is 63 characters per RFC 1034
        r'((?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+)(?:[A-Z0-9-]{2,63}(?<!-))$',
        re.IGNORECASE
    )
    literal_regex = re.compile(
        # literal form, ipv4 or ipv6 address (SMTP 4.1.3)
        r'\[([A-f0-9:\.]+)\]$',
        re.IGNORECASE
    )
    ipv4 = re.compile(r'^(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$')
    domain_whitelist = ['localhost']

    def validate(self, value):
        if not value or '@' not in value:
            self.error(self.message)

        user_part, domain_part = value.rsplit('@', 1)

        if not self.user_regex.match(user_part):
            self.error(self.message)

        if domain_part not in self.domain_whitelist and not self.validate_domain_part(domain_part):
            # Try for possible IDN domain-part
            try:
                domain_part = domain_part.encode('idna').decode('ascii')
                if self.validate_domain_part(domain_part):
                    return
            except UnicodeError:
                pass
            self.error(self.message)

    def validate_domain_part(self, domain_part):
        from .exceptions import ValidationError

        if self.domain_regex.match(domain_part):
            return True

        literal_match = self.literal_regex.match(domain_part)
        if literal_match:
            ip_address = literal_match.group(1)
            if not self.is_valid_ipv4(ip_address):
                if not self.is_valid_ipv6(ip_address):
                    return False
        return True

    def is_valid_ipv4(self, ip_str):
        return bool(self.ipv4.search(ip_str))

    def is_valid_ipv6(self, ip_str):
        # We need to have at least one ':'.
        if ':' not in ip_str:
            return False

        # We can only have one '::' shortener.
        if ip_str.count('::') > 1:
            return False

        # '::' should be encompassed by start, digits or end.
        if ':::' in ip_str:
            return False

        # A single colon can neither start nor end an address.
        if ((ip_str.startswith(':') and not ip_str.startswith('::')) or
                (ip_str.endswith(':') and not ip_str.endswith('::'))):
            return False

        # We can never have more than 7 ':' (1::2:3:4:5:6:7:8 is invalid)
        if ip_str.count(':') > 7:
            return False

        # If we have no concatenation, we need to have 8 fields with 7 ':'.
        if '::' not in ip_str and ip_str.count(':') != 7:
            # We might have an IPv4 mapped address.
            if ip_str.count('.') != 3:
                return False

        ip_str = self._explode_shorthand_ip_string(ip_str)

        # Now that we have that all squared away, let's check that each of the
        # hextets are between 0x0 and 0xFFFF.
        for hextet in ip_str.split(':'):
            if hextet.count('.') == 3:
                # If we have an IPv4 mapped address, the IPv4 portion has to
                # be at the end of the IPv6 portion.
                if not ip_str.split(':')[-1] == hextet or not self.is_valid_ipv4(hextet):
                    return False
            else:
                try:
                    # a value error here means that we got a bad hextet,
                    # something like 0xzzzz
                    if int(hextet, 16) < 0x0 or int(hextet, 16) > 0xFFFF:
                        return False
                except ValueError:
                    return False

    def _explode_shorthand_ip_string(self, ip_str):
        """
        Expand a shortened IPv6 address.
        Args:
            ip_str: A string, the IPv6 address.
        Returns:
            A string, the expanded IPv6 address.
        """
        if not self._is_shorthand_ip(ip_str):
            # We've already got a longhand ip_str.
            return ip_str

        new_ip = []
        hextet = ip_str.split('::')

        # If there is a ::, we need to expand it with zeroes
        # to get to 8 hextets - unless there is a dot in the last hextet,
        # meaning we're doing v4-mapping
        if '.' in ip_str.split(':')[-1]:
            fill_to = 7
        else:
            fill_to = 8

        if len(hextet) > 1:
            sep = len(hextet[0].split(':')) + len(hextet[1].split(':'))
            new_ip = hextet[0].split(':')

            for __ in range(fill_to - sep):
                new_ip.append('0000')
            new_ip += hextet[1].split(':')

        else:
            new_ip = ip_str.split(':')

        # Now need to make sure every hextet is 4 lower case characters.
        # If a hextet is < 4 characters, we've got missing leading 0's.
        ret_ip = []
        for hextet in new_ip:
            ret_ip.append(('0' * (4 - len(hextet)) + hextet).lower())
        return ':'.join(ret_ip)

    def _is_shorthand_ip(ip_str):
        """
        Determine if the address is shortened.

        Args:
            ip_str: A string, the IPv6 address.
        Returns:
            A boolean, True if the address is shortened.
        """
        if ip_str.count('::') == 1:
            return True
        if any(len(x) < 4 for x in ip_str.split(':')):
            return True
        return False


class PickleProperty(BlobProperty):
    """Store data as pickle. Takes care of (un)pickling."""
    def to_protobuf(self, value):
        return super(PickleProperty, self).to_protobuf(pickle.dumps(value, protocol=2))  # Py2 compatible

    def from_protobuf(self, pb_value):
        return pickle.loads(super(PickleProperty, self).from_protobuf(pb_value))


class DictProperty(PickleProperty):
    """
    Store a python dict.

    Dict is pickled for storage.
    """
    def validate(self, value):
        if not isinstance(value, dict):
            self.error("Value for a DictProperty must be a dict instance. Got %s instead." % type(value))


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

    def from_protobuf(self, pb_value):
        microseconds = pb_value.timestamp_microseconds_value
        naive = (datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(microseconds=microseconds))
        return naive.replace(tzinfo=pytz.utc).date()

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

    def _now(self):
        return datetime.datetime.utcnow().date()


class TimeProperty(DateTimeProperty):
    """Store data as time represented using datetime.time."""
    def _validate(self, value):
        if not isinstance(value, datetime.time):
            self.error("Value must be a datetime.time")
        return value

    def from_protobuf(self, pb_value):
        microseconds = pb_value.timestamp_microseconds_value
        naive = (datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(microseconds=microseconds))
        return naive.replace(tzinfo=pytz.utc).time()

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


class ListProperty(ContainerBaseProperty):
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
        if not isinstance(property, BaseProperty):
            raise TypeError('property must be a BaseProperty Instance')
        if isinstance(property, (KeyProperty, ListProperty)):
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
            self.property.validate(item)
