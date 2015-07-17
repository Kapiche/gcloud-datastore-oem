# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import

from ..exceptions import ValidationError


NON_FIELD_ERRORS = '__all__'


class BaseEntity(object):

    def __init__(self, **kwargs):
        """
        Initialise an entity. Values for an entity's properties can be passed as keyword or positional arguments.

        All entities store the key under a reserved ``key`` property. Property names can't start with '_'.

        :type key: :class:`Entity` or tuple or str or int
        :param key: The key for this Entity. If this is another that Entity will be assigned as the parent of this
                    Entity. If its a tuple, it should be in the form (parent_entity, key_value). Otherwise, it should be
                    the value of this key (either an int or str).
        """
        self._initialised = False
        self._data = {}  # Property values will be stored here by the property descriptors

        # Set attribute values
        for name in self._properties:
            setattr(self, name, kwargs.get(name, None))  # None causes default value to be set if present

        self._initialised = True
        self._meta.pk = self.pk  # Django compatibility hack

    @property
    def pk(self):
        return self.key.name_or_id

    def clean(self):
        """
        Hook for doing document level data cleaning before validation is run.

        Any ValidationError raised by this method will not be associated with a particular field; it will have a
        special-case association with the field defined by NON_FIELD_ERRORS.
        """
        pass

    def validate(self, clean=True):
        """
        Validate all properties on this entity and (optionally) this entity itself.

        :param bool clean: Whether to perform entity level validation via :meth:`clean()`.
        :raise :exc:`~gcloudoem.queryset.errors.ValidationError`: If validation fails.
        """
        errors = {}
        if clean:
            try:
                self.clean()
            except ValidationError as e:
                errors[NON_FIELD_ERRORS] = e
        for field, value in [(self._properties[name], self._data[name]) for name in self._properties]:
            try:
                field._validate(value)
            except ValidationError as error:
                errors[field.name] = error.errors or error
            except (ValueError, AttributeError, AssertionError) as error:
                errors[field.name] = error

        if errors:
            message = "ValidationError (%s:%s) " % (self.__class__, self.key.name_or_id)
            raise ValidationError(message, errors=errors)

    @classmethod
    def from_protobuf(cls, pb):
        """
        Get an instance of this class based on the protobug representation from the Datastore API.

        :type pb: :class:`gcloudoem.datastore.datastore_v1_pb2.Entity`
        :param pb: The protobuf representing the entity.

        :return: Instance of cls.
        """
        raise NotImplementedError
