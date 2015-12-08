# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import warnings
import itertools
import sys

import six

from ..datastore.query import Query
from ..datastore.transaction import Transaction
from ..exceptions import GCloudError
from ..utils import VERSION_PICKLE_KEY
from .lookups import convert_lookups, LOOKUP_SEP


# The maximum number of items to display in a QuerySet.__repr__
REPR_OUTPUT_SIZE = 20


class QuerySet(object):
    """
    A set of results returned from a query.

    Internally, this just creates a :class:`~gcloudoem.datastore.query.Query`, and returns
    :class:`~gcloudoem.entity.Entity` instances from the underlying :class:`~gcloudoem.data.query.Cursor`.
    """
    def __init__(self, entity, queries=None):
        self.entity = entity
        self._result_cache = None

        self._queries = queries or []
        self._properties = None

        self._start = 0
        self._limit = None
        self._step = 1
        self._order = None
        self._is_filtered = False
        self._projection = None

    ##
    # Python data-model related functions
    ##
    def __getstate__(self):
        """Allows the QuerySet to be pickled."""
        # Force the cache to be fully populated.
        from .. import VERSION
        self._fetch_all()
        obj_dict = self.__dict__.copy()
        obj_dict[VERSION_PICKLE_KEY] = VERSION
        return obj_dict

    def __setstate__(self, state):
        from .. import VERSION
        msg = None
        pickled_version = state.get(VERSION_PICKLE_KEY)
        if pickled_version:
            current_version = VERSION
            if current_version != pickled_version:
                msg = ("Pickled queryset instance's Gcloudoem version %s does not match the current version %s." %
                       (pickled_version, current_version))
        else:
            msg = "Pickled queryset instance's Gcloudoem version is not specified."

        if msg:
            warnings.warn(msg, RuntimeWarning, stacklevel=2)

        self.__dict__.update(state)

    def __deepcopy__(self, memo):
        """Deep copy of a QuerySet doesn't populate the cache."""
        obj = self.__class__()
        for k, v in self.__dict__.items():
            if k == '_result_cache':
                obj.__dict__[k] = None
            else:
                obj.__dict__[k] = copy.deepcopy(v, memo)
        return obj

    def __getitem__(self, k):
        """Support skip and limit using getitem and slicing syntax."""
        queryset = self._clone()

        if not isinstance(k, (slice,) + six.integer_types):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0)) or
                (isinstance(k, slice) and (k.start is None or k.start >= 0) and
                 (k.stop is None or k.stop >= 0))), "Negative indexing is not supported."

        # Slice provided
        if isinstance(k, slice):
            qs = self._clone()
            qs._start, qs._limit, = k.start, k.stop
            if k.start and k.stop:
                qs._limit = k.stop - k.start
            qs._step = k.step
            return list(qs)[qs._start:qs._limit:qs._step]
        # Integer index provided
        elif isinstance(k, six.integer_types):
            qs = self._clone()
            return list(qs)[k]

    def __iter__(self):
        """Fills the cache by evaluating the queryset then iterates the results."""
        self._fetch_all()
        return iter(self._result_cache)

    def __nonzero__(self):
        """ Avoid to open all records in an if stmt in Py2. """
        return type(self).__bool__(self)

    def __bool__(self):
        self._fetch_all()
        return bool(self._result_cache)

    def __repr__(self):
        data = list(self[:REPR_OUTPUT_SIZE + 1])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)

    def __len__(self):
        self._fetch_all()
        return len(self._result_cache)

    ##
    # Public methods that evaluate the queryset
    ##
    def iterator(self):
        for q in self._queries:
            if self._order:
                q.order = self._order
            if self._projection:
                q.projections(self._projection)
        return itertools.chain(*[q() for q in self._queries])

    def count(self):
        """
        Returns the number of entities in the queryset as an integer.

        This results in this queryset being evaluated.
        """
        self._fetch_all()
        return len(self._result_cache)

    def get(self, *args, **kwargs):
        """
        Performs the query and returns a single entity matching the given keyword arguments.

        if strong_consistency is True, This is done inside a Datastore transaction.
        """
        strong_consistency = kwargs.pop('strong_consistency', False)
        clone = self.filter(*args, **kwargs)
        clone = clone.order_by()
        try:
            if strong_consistency:
                transaction = Transaction(Transaction.SERIALIZABLE)
                transaction.begin()
            num = len(clone)
            if num == 1:
                return clone._result_cache[0]
            if not num:
                raise self.entity.DoesNotExist("%s matching query does not exist." % self.entity._meta.kind)
            raise self.entity.MultipleObjectsReturned(
                "get() returned more than one %s -- it returned %s!" % (self.entity._meta.kind, num)
            )
        finally:
            if strong_consistency:
                transaction.commit()

    def create(self, **kwargs):
        """Creates a new entity with the given kwargs, saving it to the database and returning the created entity."""
        entity = self.entity(**kwargs)
        entity.save(force_insert=True)
        return entity

    def bulk_create(self, entities):
        """
        Inserts each of the instances into the database. This does *not* call save() on each of the instances (save()
        runs in its own transaction), but the outcome is the same as if you were to call save() on each entity. Rather,
        it saves all the entities in Datastore within the one transaction.

        :type entities: iterable of :class:`~gcloudoem.entity.Entity` instances.
        :param entities: The entities to save.

        :return: The created entities
        """
        for chunk in self._chunk(entities, 500):
            with Transaction(Transaction.SNAPSHOT) as txn:
                for entity in chunk:
                    txn.create(entity)

        return entities

    def get_or_create(self, defaults=None, **kwargs):
        """
        Looks up an entity with the given kwargs, creating one if necessary.

        Returns a tuple of (object, created), where created is a boolean specifying whether an object was created.

        :rtype: tuple
        """
        lookup, params = self._extract_model_params(defaults, **kwargs)
        try:
            return self.get(**lookup), False
        except self.entity.DoesNotExist:
            return self._create_object_from_params(lookup, params)

    def update_or_create(self, defaults=None, **kwargs):
        """
        Looks up an entity with the given kwargs, updating one with defaults if it exists, otherwise creates a new one.


        :rtype: tuple
        :returns: tuple (entity, created), where created is a boolean specifying whether an entity was created.
        """
        defaults = defaults or {}
        lookup, params = self._extract_model_params(defaults, **kwargs)
        try:
            entity = self.get(**lookup)
        except self.entity.DoesNotExist:
            entity, created = self._create_object_from_params(lookup, params)
            if created:
                return entity, created
        for k, v in six.iteritems(defaults):
            setattr(entity, k, v)

        entity.save()
        return entity, False

    def earliest(self, field_name=None):
        return self._earliest_or_latest(field_name=field_name, direction="")

    def latest(self, field_name=None):
        return self._earliest_or_latest(field_name=field_name, direction="-")

    def first(self):
        """
        Returns the first object of a query, returns None if no match is found.
        """
        entities = list(self)[:1]
        if entities:
            return entities[0]
        return None

    def last(self):
        """
        Returns the last object of a query, returns None if no match is found.
        """
        objects = list(self.reverse())[:1]
        if objects:
            return objects[0]
        return None

    def in_bulk(self, id_list):
        """
        Returns a dictionary mapping each of the given IDs to the entity with that ID.

        For this method, ID means the name_or_id of an entity key.
        """
        assert not self._is_limited(), "Can't 'limit' or 'offset' with in_bulk"
        if not id_list:
            return {}
        qs = self.filter(pk__in=id_list).order_by()
        return {e.key.name_or_id: e for e in qs}

    def delete(self):
        """Deletes the entities in the current QuerySet."""
        assert not self._is_limited(), "Cannot use 'limit' or 'offset' with delete."

        if self._properties is not None:
            raise TypeError("Cannot call delete() after .values() or .values_list()")

        for chunk in self._chunk(list(self._clone()), 500):  # Can only operate on 500 items at once
            with Transaction(Transaction.SNAPSHOT) as txn:
                for e in iter(chunk):
                    txn.delete(e)

        # Clear the result cache, in case this QuerySet gets reused.
        self._result_cache = None

    def update(self, **kwargs):
        """Updates all elements in the current QuerySet, setting all the given properties to the appropriate values."""
        assert not self._is_limited(), "Cannot update a query once a slice has been taken."
        entities = list(self)
        for e in entities:
            for name, value in kwargs.items():
                setattr(e, name, value)
        for chunk in self._chunk(entities, 500):
            with Transaction(Transaction.SNAPSHOT) as txn:
                for e in entities:
                    txn.put(e)
        self._result_cache = None
        return entities

    def exists(self):
        return self.__bool__()

    ##
    # Public methods that return an new queryset
    ##
    def all(self):
        """
        Returns a new QuerySet that is a copy of the current one. This allows a QuerySet to proxy for a model manager
        in some cases.
        """
        return self._clone()

    def filter(self, *args, **kwargs):
        """Returns a new QuerySet instance with the args ANDed to the existing set."""
        return self._filter_or_exclude(False, *args, **kwargs)

    def exclude(self, *args, **kwargs):
        """Returns a new QuerySet instance with NOT (args) ANDed to the existing set."""
        # return self._filter_or_exclude(True, *args, **kwargs)
        raise NotImplementedError

    def order_by(self, *field_names):
        """
        Returns a new QuerySet instance with the ordering changed.
        """
        assert not self._is_limited(), "Cannot reorder a query once a slice has been taken."
        obj = self._clone()
        obj._order = field_names
        return obj

    def projection(self, *properties):
        """Use a Datastore projection query. A projection query only returns certain properties as specified."""
        assert not self._is_limited(), "Cannot reorder a query once a slice has been taken."
        assert not self._projection, "projection() already used."
        clone = self._clone()
        clone._projection = properties
        return clone

    def keys_only(self):
        """Retrieve only the key property for entities in the QuerySet."""
        assert not self._is_limited(), "Cannot reorder a query once a slice has been taken."
        assert not self._projection, "Can't use keys_only() after using projection()."
        clone = self._clone()
        clone._projection = '__key__'
        return clone

    ##
    # Private methods
    ##
    def _clone(self, **kwargs):
        clone = self.__class__(entity=self.entity, queries=self._queries[:])
        clone._properties = self._properties
        clone._start = self._start
        clone._limit = self._limit
        clone._step = self._step
        clone._is_filtered = self._is_filtered
        clone._projection = self._projection

        clone.__dict__.update(kwargs)

        return clone

    def _fetch_all(self):
        """
        Evaluates this query set and populates the cache. Does nothing is the cache is already populated.

        Ultimately, this just ends up evaluating the underlying :class:`~gcloudoem.datastore.query.Quert` and saving
        the results returned by the :class:`~gcloudoem.datastore.query.Cursor`.
        """
        if not self._result_cache:
            self._result_cache = list(self.iterator())

    def _has_filters(self):
        """
        Checks if this QuerySet has any filtering going on. Note that this isn't equivalent for checking if all objects
        are present in results, for example qs[1:]._has_filters() -> False.
        """
        return self._is_filtered

    def _is_limited(self):
        """
        Checks if the QuerySet has and limiting/slicing happening.

        :return: bool
        """
        return self._start or self._limit

    def _create_object_from_params(self, lookup, params):
        """Tries to create an entity using passed params. Used by get_or_create and update_or_create."""
        try:
            return self.create(**params), True
        except GCloudError:
            exc_info = sys.exc_info()
            try:
                return self.get(**lookup), False
            except self.entity.DoesNotExist:
                pass
            six.reraise(*exc_info)

    def _extract_model_params(self, defaults, **kwargs):
        """
        Prepares `lookup` (kwargs that are valid model attributes), `params` (for creating a model instance) based on
        given kwargs; for use by get_or_create and update_or_create.

        Really, all this does is remove `lookup` args from `params` and applies `defaults` to params.
        """
        defaults = defaults or {}
        lookup = kwargs.copy()
        params = {k: v for k, v in kwargs.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        return lookup, params

    def _earliest_or_latest(self, field_name=None, direction="-"):
        """Returns the latest entity, according to the model's 'get_latest_by' option or optional given field_name."""
        order_by = field_name or getattr(self.entity._meta, 'get_latest_by')
        assert bool(order_by), \
            "earliest() and latest() require either a field_name parameter or 'get_latest_by' in the model"
        assert not self._is_limited(), "Can't apply limit or slice to earliest() or latest()"
        clone = self._clone()
        clone._stop = 1
        clone.order_by('%s%s' % (direction, order_by))
        return clone.get()

    def _filter_or_exclude(self, negate, *args, **kwargs):
        if args or kwargs:
            assert not self._is_limited(), "Cannot filter a query once a slice has been taken."
        assert not negate, "Exclude not supported yet"

        clone = self._clone()
        filters = convert_lookups(**kwargs)
        for f in filters:
            # Datastore doesn't support OR queries. To do OR queries, you need to do separate queries.
            # So each AND filter gets applied to every query. Each OR filter (only the in operator) creates a new query.
            if f[1] == 'in':
                existing_filters = clone._queries[0].filters() if clone._queries else []
                clone._queries = clone._queries + [
                    Query(self.entity, filters=existing_filters + [(f[0], 'eq', value,)]) for value in f[2]
                ]
            else:
                if clone._queries:
                    for q in clone._queries:
                        q.add_filter(*f)
                else:
                    clone._queries.append(Query(self.entity, filters=[f]))
        return clone

    @staticmethod
    def _chunk(items, size):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(items), size):
            yield items[i:i+size]
