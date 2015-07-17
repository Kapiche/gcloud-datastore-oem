# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
"""
:class:`.Entity`s are the building blocks of the OEM layer, providing object-orientated interaction with datastore
entities.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

from future.utils import with_metaclass

from .base.entity import BaseEntity
from .base.metaclasses import EntityMeta
from .datastore.transaction import Transaction


class Entity(with_metaclass(EntityMeta, BaseEntity)):
    """
    A :class:`Entity` is just a mapping of an object to a datastore entity. ``cls.__name__`` is used as the kind for
    the datastore entity. A :class:`Entity` has 1 or more properties as its attributes which are persisted to the
    datastore as  properties of a datastore entity. See :mod:`gcloudoem.properties` for a full list of available
    properties. For example:

        from gcloudoem import entity, properties

        class Person(entity.Entity):
            name = properties.TextProperty(indexed=False)
            dob = properties.DateProperty()

    The id/name for the key of the underlying entity will be automatically inferred from the Entity instance using the
    following strategy:

    * A :class:'~gcloudoem.properties.KeyProperty' stored on the ``key`` attribute. Otherwise;
    * A ``key`` attribute of type :class:`~gcloudoem.properties.KeyProperty` will be injected onto the instance.

    If more then one :class:'~gcloudoem.properties.KeyProperty' is present on the Entity, an AttributeError will be
    thrown.

    Access to the entity's key is via :attr:`.key`, and the id can be fetched using ``.key.id_or_name``.

    To save/update an model, call :func:`~.save` on it. To fetch a model by id, call :func:`~.get_by_id`. To fetch
    multiple model instances at once, use :func:`~.filter`. To delete a model instance from datastore, call :
    func:`~.delete`.

    This class shouldn't be used directly. Instead, it is intended to be extended by concrete model implementations.
    """
    def __init__(self, **kwargs):
        super(Entity, self).__init__(**kwargs)

    def save(self, force_insert=False, validate=True, clean=True, **kwargs):
        """
        Save the :class:`Entity` to the database. If the entity already exists, it will be updated,
        otherwise it will be created.

        :param force_insert: only try to create a new document, don't allow updates of existing documents. Defaults to
            False.
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be True.

        """
        if validate:
            self.validate(clean=clean)

        with Transaction(Transaction.SNAPSHOT) as txn:
            if force_insert:
                txn.create(self)
            else:
                txn.put(self)

    def delete(self):
        """Delete this entity from Datastore."""
        with Transaction(Transaction.SNAPSHOT) as txn:
            txn.delete(self)

    @classmethod
    def from_protobuf(cls, pb):
        key = None
        if pb.HasField('key'):
            key = cls._properties['key'].from_protobuf(pb.key)

        entity_props = {
            'key': key
        }

        for property_pb in pb.property:
            if not hasattr(cls, property_pb.name):
                raise ValueError("Entity %s doesn't have a property by the name %s" %
                                 (cls._meta.kind, property_pb.name))
            value = cls._properties[property_pb.name].from_protobuf(property_pb.value)
            entity_props[property_pb.name] = value

        instance = cls(**entity_props)
        return instance

    def __repr__(self):
        key = self._data['key']
        return "<%s (%s) %s)>" % (
            self.__class__.__name__,
            key.name_or_id,
            super(Entity, self).__repr__()
        )
