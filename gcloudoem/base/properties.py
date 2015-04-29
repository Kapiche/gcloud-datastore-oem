# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import
from __future__ import absolute_import

import six

from ..exceptions import ValidationError


class BaseProperty(object):
    """
    The base class for properties in Datastore. Instances of this class may be added to subclasses of `Entity` to store
    data.

    A property has a value, can have validators and a default value. Useful attributes of a property include:

    * :attr:`db_field` the name of this property in the Datastore.

    This class shouldn't be used directly. Instead, it is intended to be extended by concrete property implementations.
    """
    def __init__(self, name=None, db_name=None, required=False, default=None, choices=None, help_text=None,
                 verbose_name=None, exclude_from_index=False):
        """
        Initialise a property.

        :param str name: The name used for this property on the entity. Defaults to the the attribute name used for
            this property on an :class:`~gcloudoem.entity.Entity`.
        :param str db_name: The datastore name used for this property. Defaults to `name`.
        :param bool required: Is this property is required? Defaults to False.
        :param default: (optional) The default value of this property of no value has been set? Can be a callable.
        :param list choices: (optional) A list of values this property should have.
        :param str help_text: (optional) The help text for this property. Might be used by implementers of a GUI.
        :param str verbose_name: The verbose name for this property. Designed to be human readable. Might be used by
            implementers of a GUI.
        :param bool exclude_from_index: Weather to exclude this property from the entity's index.
        """
        self.name = self.db_name = name
        self.db_name = db_name
        self.required = required
        self.default = default
        self.choices = choices
        self.help_text = help_text
        self.verbose = verbose_name
        self.exclude_from_index = exclude_from_index

    def __get__(self, instance, owner):
        if not instance:  # being called on an entity class
            return self
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        if value is None and self.default is not None:
            value = self.default
            if callable(value):
                value = value()
        instance._data[self.name] = value

    def from_protobuf(self, pb_value):
        """
        Given a protobuf value for a Property, get the correct value.

        The Cloud Datastore Protobuf API returns a Property Protobuf which has one value set and the rest blank.
        This function retrieves the the one value provided.

        Some work is done to coerce the return value into a more useful python type.

        :type property_pb: :class:`gcloudoem.datastore.datastore_v1_pb2.Property`
        :param property_pb: The Property Protobuf.

        :returns: The python value provided by the Protobuf.
        """
        raise NotImplementedError

    def to_protobuf(self, value):
        """
        Given a value, return the protobuf attribute name and proper value.

        The Protobuf API uses different attribute names based on value types rather than inferring the type. This
        function simply returns the proper attribute name for this Property as well as a properly formatted value.

        Certain value types need to be coerced into a different type. This function handles that for you.

        .. note::
           Values which are "text" ('unicode' in Python2, 'str' in Python3) map to 'string_value' in the datastore;
           values which are "bytes" ('str' in Python2, 'bytes' in Python3) map to 'blob_value'.

        For example:

            >>> _pb_attr_value(1234)
            ('integer_value', 1234)
            >>> _pb_attr_value('my_string')
            ('string_value', 'my_string')

        :param object val: The value to be scrutinized.

        :rtype: tuple
        :returns: A tuple of the attribute name and proper Protobuf value type.
        """
        raise NotImplementedError

    def validate(self, value):
        """
        Validate this property.

        Should be overridden by subclasses to provide custom validation.

        :raise: :exception:`~gcloudoem.queryset.exceptions.ValidationError`.
        """
        pass

    def _validate(self, value):
        if self.choices:
            choice_list = self.choices
            if isinstance(self.choices[0], (list, tuple)):
                choice_list = [k for k, v in self.choices]

            if value not in choice_list:
                self.error('Value must be one of %s' % six.text_type(choice_list))

        if self.required and value is None:
            self.error('Value is required.')

        if value is not None:
            self.validate(value)

    def error(self, message="", errors=None, field_name=None):
        """Raises a ValidationError."""
        field_name = field_name if field_name else self.name
        raise ValidationError(message, errors=errors, field_name=field_name)


class ContainerBaseProperty(BaseProperty):
    """
    A ContainerBaseProperty is designed for use with any property that is meant to be a container of other properties
    (like a list for example). It handles correctly fetching things like ReferenceProperties etc.

    Subclasses of this class must have a ``property`` attribute which contains the BaseProperty instance this class is a
    container for.
    """
    def __get__(self, instance, owner):
        if not instance:  # being called on entity calss
            return self

        value = super(ContainerBaseProperty, self).__get__(instance, owner)

        if isinstance(value, (list, tuple)):
            from ..properties import ReferenceProperty

            if self.property and isinstance(self.property, ReferenceProperty):
                from .. import Entity, Key
                for i, k in enumerate(value):
                    if not isinstance(k, Entity):
                        value[i] = self.property.entity_cls.objects.get(pk=k.name_or_id)
                setattr(instance, self.name, value)  # cache any fetched entities
            return value
