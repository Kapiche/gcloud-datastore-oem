import warnings

from .properties import BaseProperty
from ..properties import KeyProperty


class EntityMeta(type):
    """
    Metaclass for :class:`~gcloud.entity.Entity` classes.

    Sets the name of :class:`~gcloudoem.base.base.BasePropery` class attributes and injects the
    :class:`~gcloud.properties.KeyProperty` property at ``key`` if required.
    """
    def __new__(cls, name, bases, attrs):
        if 'key' not in attrs:  # Ensure there is a key
            value = KeyProperty()
            value.db_name = 'key'
            attrs['key'] = value
        new_cls = super(EntityMeta, cls).__new__(cls, name, bases, attrs)

        # Store the properties for this entity
        new_cls._properties = {}
        for name, value in attrs.items():
            if isinstance(value, BaseProperty):
                if name == 'key' and not isinstance(value, KeyProperty):
                    raise AttributeError("Attribute of 'key' isn't allowed unless it is a KeyProperty.")
                elif isinstance(value, KeyProperty) and not name == 'key':
                    raise AttributeError("Only attr 'key' can be a KeyProperty.")
                value.db_name = name
                new_cls._properties[name] = value
        return new_cls
