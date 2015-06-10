# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from builtins import str as text

import six


class Key(object):
    """
    A Key for an Entity in Google Datastore.

    There should be no need to use this class directly. Rather, it should be used via
    :class:`~gcloudoem.properties.KeyProperty` on an :class:`~gcloud.entity.Entity`.

    Datastore keys have an ``id`` if they are int based, or a ``name`` if they are str based. You can use
    :prop:`name_or_id` to fetch the appropriate identifier for this key. Keys can also optionally have a parent which
    should be another :class:`Key`.

    If no id or name is specified for this key, then an int id will be auto assigned to it when the owning
    :class:`~gcloud.entity.Entity` is saved.
    """
    def __init__(self, kind, parent=None, value=None):
        """
        Initialise a new key.

        :param str kind: The Datastore king of this key.
        :param :class:`Key` parent: The parent of this key.
        :param int or st value: the name or id of this key.
        """
        self._kind = u"%s" % text(kind)  # Make SURE we have unicode
        self._parent = parent
        self._id = self._name = None
        if isinstance(value, six.string_types):
            self._name = value
        elif isinstance(value, six.integer_types):
            self._id = value

    @property
    def kind(self):
        return self._kind

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._id

    @property
    def name_or_id(self):
        return self._name if self._name else self._id

    @property
    def parent(self):
        return self._parent

    @property
    def is_partial(self):
        return self.name_or_id is None

    @property
    def path(self):
        """
        The path of this key, oldest ancestor first.

        :rtype: list of :class:`Key`s
        :return: The full path of this :class:`Key` which includes this key itself as the last element.
        """
        path = [self]
        key = self._parent
        while key:
            path = [key] + path
            key = key.parent
        return path
