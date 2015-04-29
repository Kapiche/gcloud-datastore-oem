# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import

from .utils import camel_case_to_spaces


class Options(object):
    """
    Store the options for an entity.

    You shouldn't need to instantiate this class directly. Rather, the :class:`~gcloudoem.base.metaclasses.EntityMeta`
    will do it for you if you define a `Meta` class on your entity. For example::

        from gcloudoem import *

        class Person(Entity):
            name = TextProperty()

            class Meta:
                kind = 'special_person'

    You can access an instance on this class on any entity via the ``_meta`` attribute. The options are as follows:

    * **app_label** - The label for the application an Entity belongs to. Used for the name of the Entity in Datastore.
        Defaults to being empty.
    * **get_latest_by** - The name of an orderable property in the entity, typically a DateProperty, DateTimeProperty,
        or IntegerProperty. This specifies the default field to use in your model Manager's latest() and earliest()
        methods.
    * **indexed_properties** - Which properties should be indexed in Datastore? This should be a tuple or list of string
        property names. Defaults to all properties.
    * **kind** - What kind should be used to store this in datastore. Defaults to the Entity class name.
    * **namespace** - The namespace to use with Datastore. By default it is blank.
    * **ordering** - The default ordering for the entity, for use when obtaining lists of entities. This is a tuple or
        list of strings. Each string is a property name with an optional "-" prefix, which indicates descending order.
        Fields without a leading "-" will be ordered ascending. Use the string "?" to order randomly.
    * **queryset_class** - The class to use as the query set. Can be used to override the one set by the manager.
    * **verbose_name** - A human-readable name for the entity, singular. Defaults to a a munged version of the class
        name: CamelCase becomes camel case.
    * **verbose_name_plural** - The plural name for the entity. Defaults to verbose_name + "s".

    This class forms the Admin interface to a entity. Using it, it's possible to find out all the information about an
    entity including it's fields (:meth:`get_fields` and :meth:`get_field`), how it is stored in Datastore (including
    its kind and namespace), which fields are indexed, etc.
    """
    DEFAULT_NAMES = (
        'verbose_name', 'verbose_name_plural', 'kind', 'ordering', 'get_latest_by',
        'order_with_respect_to', 'namespace', 'indexed_properties',
    )

    def __init__(self, meta):
        # Defaults
        self.get_by_latest = None
        self.indexed_properties = []
        self.kind = ''
        self.namespace = ''
        self.ordering = []
        self.queryset_class = None
        self.verbose_name = ''
        self.verbose_name_plural = ''
        self.app_label = ''

        # Mostly internal things
        self.entity = None
        self.object_name = ''
        self.entity_name = ''
        self.meta = meta

    def contribute_to_class(self, cls, name):
        cls._meta = self
        self.entity = cls
        # First, construct the default values for these options.
        self.object_name = self.kind = cls.__name__
        self.entity_name = self.object_name.lower()
        self.verbose_name = camel_case_to_spaces(self.object_name)

        # Store the original user-defined values for each option,
        # for use when serializing the model definition
        self.original_attrs = {}

        # Next, apply any overridden values from 'class Meta'.
        if self.meta:
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                # Ignore any private attributes that Django doesn't care about.
                # NOTE: We can't modify a dictionary's contents while looping
                # over it, so we loop over the *original* dictionary instead.
                if name.startswith('_'):
                    del meta_attrs[name]
            for attr_name in self.DEFAULT_NAMES:
                if attr_name in meta_attrs:
                    setattr(self, attr_name, meta_attrs.pop(attr_name))
                    self.original_attrs[attr_name] = getattr(self, attr_name)
                elif hasattr(self.meta, attr_name):
                    setattr(self, attr_name, getattr(self.meta, attr_name))
                    self.original_attrs[attr_name] = getattr(self, attr_name)

            # verbose_name_plural is a special case because it uses a 's' by default.
            if self.verbose_name_plural is None:
                self.verbose_name_plural = self.verbose_name + 's'

            # Prepend the app_label to the kind
            if self.app_label != '':
                self.kind = "%s_%s" % (self.app_label, self.kind)

            # Any leftover attributes must be invalid.
            if meta_attrs != {}:
                raise TypeError("'class Meta' got invalid attribute(s): %s" % ','.join(meta_attrs.keys()))
        else:
            self.verbose_name_plural = self.verbose_name + 's'
        del self.meta
