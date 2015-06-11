# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import
import sys

from ..exceptions import DoesNotExist, MultipleObjectsReturned
from ..options import Options
from ..queryset.manager import QuerySetManager
from .properties import BaseProperty


ENTITY_MODULE_NAMES = (
    'models',  # Django style
    'entities',
)


def subclass_exception(name, parents, module, attached_to=None):
    """
    Create exception subclass. Used by EntityMeta below.

    If 'attached_to' is supplied, the exception will be created in a way that allows it to be pickled, assuming the
    returned exception class will be added as an attribute to the 'attached_to' class.
    """
    class_dict = {'__module__': module}
    if attached_to is not None:
        def __reduce__(self):
            # Exceptions are special - they've got state that isn't
            # in self.__dict__. We assume it is all in self.args.
            return (unpickle_inner_exception, (attached_to, name), self.args)

        def __setstate__(self, args):
            self.args = args

        class_dict['__reduce__'] = __reduce__
        class_dict['__setstate__'] = __setstate__

    return type(name, parents, class_dict)


def unpickle_inner_exception(klass, exception_name):
    # Get the exception class from the class it is attached to:
    exception = getattr(klass, exception_name)
    return exception.__new__(exception)


class EntityMeta(type):
    """
    Metaclass for :class:`~gcloud.entity.Entity` classes.

    Sets the name of :class:`~gcloudoem.base.base.BasePropery` class attributes and injects the
    :class:`~gcloud.properties.KeyProperty` property at ``key`` if required.
    """
    def __new__(cls, name, bases, attrs):
        flattened_bases = cls._get_bases(bases)

        # Store the properties for this entity
        from ..properties import KeyProperty
        properties = {}
        # Merge all properties from subclasses
        for base in flattened_bases[::-1]:
            if hasattr(base, '_properties'):
                properties.update(base._properties)
            # Standard object mixin - merge in any Properties
            if not hasattr(base, '_meta'):
                for attr_name, attr_value in base.__dict__.items():
                    if not isinstance(attr_value, BaseProperty):
                        continue
                    attr_value.name = attr_name
                    if not attr_value.db_name:
                        attr_value.db_name = attr_name
                    properties[attr_name] = attr_value
        # Now merge the properties from the defined Entity
        for prop_name, value in attrs.items():
            if isinstance(value, BaseProperty):
                if prop_name == 'key' and not isinstance(value, KeyProperty):
                    raise AttributeError("Attribute of 'key' isn't allowed unless it is a KeyProperty.")
                elif isinstance(value, KeyProperty) and not prop_name == 'key':
                    raise AttributeError("Only attr 'key' can be a KeyProperty.")
                value.name = prop_name
                if not value.db_name:
                    value.db_name = prop_name
                properties[prop_name] = value
        if 'key' not in attrs:  # Ensure there is a key
            value = KeyProperty(name='key', db_name='__key__')
            attrs['key'] = properties['key'] = value

        attrs['_properties'] = properties

        # Create the class
        module = attrs.pop('__module__')
        new_cls = super(EntityMeta, cls).__new__(cls, name, bases, attrs)

        # Provide a default queryset unless exists or one has been set
        if 'objects' not in dir(new_cls):
            new_cls.objects = QuerySetManager()

        # Build the Meta for this entity
        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_cls, 'Meta', None)
        else:
            meta = attr_meta

        # Try to detect the app_label
        if getattr(meta, 'app_label', None) is None:
            entity_module = sys.modules[new_cls.__module__]
            package_components = entity_module.__name__.split('.')
            package_components.reverse()  # find the last occurrence of 'models' or 'entites'
            app_label_index = -1
            for name in ENTITY_MODULE_NAMES:
                try:
                    app_label_index = package_components.index(name)
                    break
                except ValueError:
                    pass
            if app_label_index > -1:
                setattr(meta, "app_label", package_components[app_label_index + 1])

        _meta = Options(meta)
        _meta.contribute_to_class(new_cls, '_meta')

        # Add some exceptions to the Entity.
        setattr(
            new_cls,
            'DoesNotExist',
            subclass_exception(str('DoesNotExist'), (DoesNotExist,), module, attached_to=new_cls)
        )
        setattr(
            new_cls,
            'MultipleObjectsReturned',
            subclass_exception(str('MultipleObjectsReturned'), (MultipleObjectsReturned,), module, attached_to=new_cls)
        )

        return new_cls

    @classmethod
    def _get_bases(cls, bases):
        if isinstance(bases, BasesTuple):
            return bases
        seen = []
        bases = cls.__get_bases(bases)
        unique_bases = (b for b in bases if not (b in seen or seen.append(b)))
        return BasesTuple(unique_bases)

    @classmethod
    def __get_bases(cls, bases):
        for base in bases:
            if base is object:
                continue
            yield base
            for child_base in cls.__get_bases(base.__bases__):
                yield child_base


class BasesTuple(tuple):
    """Special class to handle introspection of bases tuple in __new__"""
    pass
