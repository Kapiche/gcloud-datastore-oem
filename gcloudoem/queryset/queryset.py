# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import

import copy
import re
import warnings
import six

from ..datastore import Query


# The maximum number of items to display in a QuerySet.__repr__
from gcloudoem import VERSION
from gcloudoem.utils import VERSION_PICKLE_KEY

REPR_OUTPUT_SIZE = 20

RE_TYPE = type(re.compile(''))


class QuerySet(object):
    """
    A set of results returned from a query.

    Internally, this just creates a :class:`~gcloudoem.datastore.query.Query`, and returns
    :class:`~gcloudoem.entity.Entity` instances from the underlying :class:`~gcloudoem.data.query.Cursor`.
    """
    def __init__(self, entity):
        self._entity = entity
        self._results_cache = None

        self._query = Query(entity)
        self._start = self._stop = None
        self._fields = None

    #
    # Python data-model related functions
    #

    def __getstate__(self):
        """Allows the QuerySet to be pickled."""
        # Force the cache to be fully populated.
        self._fetch_all()
        obj_dict = self.__dict__.copy()
        obj_dict[VERSION_PICKLE_KEY] = VERSION
        return obj_dict

    def __setstate__(self, state):
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
        queryset = self.clone()

        if not isinstance(k, (slice,) + six.integer_types):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0))
                or (isinstance(k, slice) and (k.start is None or k.start >= 0) and (k.stop is None or k.stop >= 0))), \
            "Negative indexing is not supported."

        # Slice provided
        if isinstance(k, slice):
            qs = self._clone()
            if k.start is not None:
                self._start = int(k.start)
            else:
                self._start = None
            if k.stop is not None:
                self._stop = int(k.stop)
            else:
                self._stop = None
            return list(qs)[::k.step] if k.step else qs
        # Integer index provided
        elif isinstance(k, six.integer_types):
            self._stop = k
            return list(self._query.execute(limit=self._limit))[k]

    def __iter__(self):
        """Fills the cache by evaluating the queryset then iterates the results."""
        self._fetch_all()
        return iter(self._results_cache)

    def __nonzero__(self):
        """ Avoid to open all records in an if stmt in Py2. """
        return type(self).__bool__(self)

    def __bool__(self):
        self._fetch_all()
        return bool(self._results_cache)

    def __repr__(self):
        data = list(self[:REPR_OUTPUT_SIZE + 1])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)

    def __len__(self):
        self._fetch_all()
        return len(self._result_cache)

    #
    # Methods that evaluate the queryset
    #

    def iterator(self):
        return self._query.execute(limit=self._stop, offset=self._start)

    def count(self):
        """
        Returns the number of entities in the queryset as an integer.

        This results in this queryset being evaluated.
        """
        self._fetch_all()
        return len(self._result_cache)

    def get(self, *args, **kwargs):
        """Performs the query and returns a single entity matching the given keyword arguments."""
        clone = self.filter(*args, **kwargs)
        if self._query.can_filter():
            clone = clone.order_by()
        num = len(clone)
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise self._entity.DoesNotExist(
                "%s matching query does not exist." %
                self._entity._meta['kind']
            )
        raise self._entity.MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!" % (self._entity._meta['kind'], num)
        )

    def all(self):
        """Returns all entities."""
        return self.__call__()

    def filter(self, *q_objs, **query):
        return self.__call__(*q_objs, **query)

    def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results
        and :class:`~mongoengine.queryset.DoesNotExist` or
        `DocumentName.DoesNotExist` if no results are found.

        .. versionadded:: 0.3
        """
        queryset = self.clone()
        queryset = queryset.order_by().limit(2)
        queryset = queryset.filter(*q_objs, **query)

        try:
            result = queryset.next()
        except StopIteration:
            msg = ("%s matching query does not exist."
                   % queryset._entity._class_name)
            raise queryset._entity.DoesNotExist(msg)
        try:
            queryset.next()
        except StopIteration:
            return result

        queryset.rewind()
        message = u'%d items returned, instead of 1' % queryset.count()
        raise queryset._entity.MultipleObjectsReturned(message)

    def create(self, **kwargs):
        """Create new object. Returns the saved object instance.

        .. versionadded:: 0.4
        """
        return self._entity(**kwargs).save()

    def get_or_create(self, write_concern=None, auto_save=True,
                      *q_objs, **query):
        """Retrieve unique object or create, if it doesn't exist. Returns a
        tuple of ``(object, created)``, where ``object`` is the retrieved or
        created object and ``created`` is a boolean specifying whether a new
        object was created. Raises
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` if multiple results are found.
        A new document will be created if the document doesn't exists; a
        dictionary of default values for the new document may be provided as a
        keyword argument called :attr:`defaults`.

        .. note:: This requires two separate operations and therefore a
            race condition exists.  Because there are no transactions in
            mongoDB other approaches should be investigated, to ensure you
            don't accidentally duplicate data when using this method.  This is
            now scheduled to be removed before 1.0

        :param write_concern: optional extra keyword arguments used if we
            have to create a new document.
            Passes any write_concern onto :meth:`~mongoengine.Document.save`

        :param auto_save: if the object is to be saved automatically if
            not found.

        .. deprecated:: 0.8
        .. versionchanged:: 0.6 - added `auto_save`
        .. versionadded:: 0.3
        """
        msg = ("get_or_create is scheduled to be deprecated.  The approach is "
               "flawed without transactions. Upserts should be preferred.")
        warnings.warn(msg, DeprecationWarning)

        defaults = query.get('defaults', {})
        if 'defaults' in query:
            del query['defaults']

        try:
            doc = self.get(*q_objs, **query)
            return doc, False
        except self._entity.DoesNotExist:
            query.update(defaults)
            doc = self._entity(**query)

            if auto_save:
                doc.save(write_concern=write_concern)
            return doc, True

    def first(self):
        """Retrieve the first object matching the query.
        """
        queryset = self.clone()
        try:
            result = queryset[0]
        except IndexError:
            result = None
        return result

    def insert(self, doc_or_docs, load_bulk=True, write_concern=None):
        """bulk insert documents

        :param docs_or_doc: a document or list of documents to be inserted
        :param load_bulk (optional): If True returns the list of document
            instances
        :param write_concern: Extra keyword arguments are passed down to
                :meth:`~pymongo.collection.Collection.insert`
                which will be used as options for the resultant
                ``getLastError`` command.  For example,
                ``insert(..., {w: 2, fsync: True})`` will wait until at least
                two servers have recorded the write and will force an fsync on
                each server being written to.

        By default returns document instances, set ``load_bulk`` to False to
        return just ``ObjectIds``

        .. versionadded:: 0.5
        """
        Document = _import_class('Document')

        if write_concern is None:
            write_concern = {}

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, Document) or issubclass(docs.__class__, Document):
            return_one = True
            docs = [docs]

        raw = []
        for doc in docs:
            if not isinstance(doc, self._entity):
                msg = ("Some documents inserted aren't instances of %s"
                       % str(self._entity))
                raise OperationError(msg)
            if doc.pk and not doc._created:
                msg = "Some documents have ObjectIds use doc.update() instead"
                raise OperationError(msg)
            raw.append(doc.to_mongo())

        signals.pre_bulk_insert.send(self._entity, documents=docs)
        try:
            ids = self._collection.insert(raw, **write_concern)
        except pymongo.errors.DuplicateKeyError, err:
            message = 'Could not save document (%s)'
            raise NotUniqueError(message % unicode(err))
        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', unicode(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = u'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % unicode(err))
            raise OperationError(message % unicode(err))

        if not load_bulk:
            signals.post_bulk_insert.send(
                self._entity, documents=docs, loaded=False)
            return return_one and ids[0] or ids

        documents = self.in_bulk(ids)
        results = []
        for obj_id in ids:
            results.append(documents.get(obj_id))
        signals.post_bulk_insert.send(
            self._entity, documents=results, loaded=True)
        return return_one and results[0] or results

    def delete(self, write_concern=None, _from_doc_delete=False):
        """Delete the documents matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param _from_doc_delete: True when called from document delete therefore
            signals will have been triggered so don't loop.

        :returns number of deleted documents
        """
        queryset = self.clone()
        doc = queryset._entity

        if write_concern is None:
            write_concern = {}

        # Handle deletes where skips or limits have been applied or
        # there is an untriggered delete signal
        has_delete_signal = signals.signals_available and (
            signals.pre_delete.has_receivers_for(self._entity) or
            signals.post_delete.has_receivers_for(self._entity))

        call_document_delete = (queryset._skip or queryset._limit or
                                has_delete_signal) and not _from_doc_delete

        if call_document_delete:
            cnt = 0
            for doc in queryset:
                doc.delete(write_concern=write_concern)
                cnt += 1
            return cnt

        delete_rules = doc._meta.get('delete_rules') or {}
        # Check for DENY rules before actually deleting/nullifying any other
        # references
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get('abstract'):
                continue
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == DENY and document_cls.objects(
                    **{field_name + '__in': self}).count() > 0:
                msg = ("Could not delete document (%s.%s refers to it)"
                       % (document_cls.__name__, field_name))
                raise OperationError(msg)

        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get('abstract'):
                continue
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == CASCADE:
                ref_q = document_cls.objects(**{field_name + '__in': self})
                ref_q_count = ref_q.count()
                if (doc != document_cls and ref_q_count > 0
                    or (doc == document_cls and ref_q_count > 0)):
                    ref_q.delete(write_concern=write_concern)
            elif rule == NULLIFY:
                document_cls.objects(**{field_name + '__in': self}).update(
                    write_concern=write_concern, **{'unset__%s' % field_name: 1})
            elif rule == PULL:
                document_cls.objects(**{field_name + '__in': self}).update(
                    write_concern=write_concern,
                    **{'pull_all__%s' % field_name: self})

        result = queryset._collection.remove(queryset._query, **write_concern)
        return result["n"]

    def update(self, upsert=False, multi=True, write_concern=None,
               full_result=False, **update):
        """Perform an atomic update on the fields matched by the query.

        :param upsert: Any existing document with that "_id" is overwritten.
        :param multi: Update multiple documents.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param full_result: Return the full result rather than just the number
            updated.
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        if not update and not upsert:
            raise OperationError("No update parameters, would remove data")

        if write_concern is None:
            write_concern = {}

        queryset = self.clone()
        query = queryset._query
        update = transform.update(queryset._entity, **update)

        # If doing an atomic upsert on an inheritable class
        # then ensure we add _cls to the update operation
        if upsert and '_cls' in query:
            if '$set' in update:
                update["$set"]["_cls"] = queryset._entity._class_name
            else:
                update["$set"] = {"_cls": queryset._entity._class_name}
        try:
            result = queryset._collection.update(query, update, multi=multi,
                                                 upsert=upsert, **write_concern)
            if full_result:
                return result
            elif result:
                return result['n']
        except pymongo.errors.DuplicateKeyError, err:
            raise NotUniqueError(u'Update failed (%s)' % unicode(err))
        except pymongo.errors.OperationFailure, err:
            if unicode(err) == u'multi not coded yet':
                message = u'update() method requires MongoDB 1.1.3+'
                raise OperationError(message)
            raise OperationError(u'Update failed (%s)' % unicode(err))

    def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.

        :param object_ids: a list or tuple of ``ObjectId``\ s
        :rtype: dict of ObjectIds as keys and collection-specific
                Document subclasses as values.

        .. versionadded:: 0.3
        """
        doc_map = {}

        docs = self._collection.find({'_id': {'$in': object_ids}},
                                     **self._cursor_args)
        if self._scalar:
            for doc in docs:
                doc_map[doc['_id']] = self._get_scalar(
                    self._entity._from_son(doc, only_fields=self.only_fields))
        elif self._as_pymongo:
            for doc in docs:
                doc_map[doc['_id']] = self._get_as_pymongo(doc)
        else:
            for doc in docs:
                doc_map[doc['_id']] = self._entity._from_son(doc,
                                                             only_fields=self.only_fields,
                                                             _auto_dereference=self._auto_dereference)

        return doc_map

    def none(self):
        """Helper that just returns a list"""
        queryset = self.clone()
        queryset._none = True
        return queryset

    def using(self, alias):
        """
        This method is for controlling which database the QuerySet will be evaluated against if you are using more than
        one database.

        :param alias: The database alias
        """
        raise NotImplementedError

    def clone(self):
        """Creates a copy of the current :class:`~mongoengine.queryset.QuerySet`"""
        return self.clone_into(self.__class__(self._entity))

    def clone_into(self, cls):
        """Creates a copy of the current
          :class:`~mongoengine.queryset.base.BaseQuerySet` into another child class
        """
        if not isinstance(cls, BaseQuerySet):
            raise OperationError(
                '%s is not a subclass of BaseQuerySet' % cls.__name__)

        copy_props = ('_mongo_query', '_initial_query', '_none', '_query_obj',
                      '_where_clause', '_loaded_fields', '_ordering', '_snapshot',
                      '_timeout', '_class_check', '_slave_okay', '_read_preference',
                      '_iter', '_scalar', '_as_pymongo', '_as_pymongo_coerce',
                      '_limit', '_skip', '_hint', '_auto_dereference',
                      '_search_text', 'only_fields', '_max_time_ms')

        for prop in copy_props:
            val = getattr(self, prop)
            setattr(cls, prop, copy.copy(val))

        if self._cursor_obj:
            cls._cursor_obj = self._cursor_obj.clone()

        return cls

    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects or
        :class:`~bson.object_id.ObjectId` a maximum depth in order to cut down
        the number queries to mongodb.

        .. versionadded:: 0.5
        """
        # Make select related work the same for querysets
        max_depth += 1
        queryset = self.clone()
        return queryset._dereference(queryset, max_depth=max_depth)

    def distinct(self, field):
        """Return a list of distinct values for a given field.

        :param field: the field to select distinct values from

        .. note:: This is a command and won't take ordering or limit into
           account.

        .. versionadded:: 0.4
        .. versionchanged:: 0.5 - Fixed handling references
        .. versionchanged:: 0.6 - Improved db_field refrence handling
        """
        queryset = self.clone()
        try:
            field = self._fields_to_dbfields([field]).pop()
        finally:
            distinct = self._dereference(queryset._cursor.distinct(field), 1,
                                         name=field, instance=self._entity)

            doc_field = self._entity._fields.get(field.split('.', 1)[0])
            instance = False
            # We may need to cast to the correct type eg. ListField(EmbeddedDocumentField)
            EmbeddedDocumentField = _import_class('EmbeddedDocumentField')
            ListField = _import_class('ListField')
            GenericEmbeddedDocumentField = _import_class('GenericEmbeddedDocumentField')
            if isinstance(doc_field, ListField):
                doc_field = getattr(doc_field, "field", doc_field)
            if isinstance(doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)):
                instance = getattr(doc_field, "document_type", False)
            # handle distinct on subdocuments
            if '.' in field:
                for field_part in field.split('.')[1:]:
                    # if looping on embedded document, get the document type instance
                    if instance and isinstance(doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)):
                        doc_field = instance
                    # now get the subdocument
                    doc_field = getattr(doc_field, field_part, doc_field)
                    # We may need to cast to the correct type eg. ListField(EmbeddedDocumentField)
                    if isinstance(doc_field, ListField):
                        doc_field = getattr(doc_field, "field", doc_field)
                    if isinstance(doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)):
                        instance = getattr(doc_field, "document_type", False)
            if instance and isinstance(doc_field, (EmbeddedDocumentField,
                                                   GenericEmbeddedDocumentField)):
                distinct = [instance(**doc) for doc in distinct]
            return distinct

    def only(self, *fields):
        """Load only a subset of this document's fields. ::

            post = BlogPost.objects(...).only("title", "author.name")

        .. note :: `only()` is chainable and will perform a union ::
            So with the following it will fetch both: `title` and `author.name`::

                post = BlogPost.objects.only("title").only("author.name")

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any field filters.

        :param fields: fields to include
        """
        fields = dict([(f, QueryFieldList.ONLY) for f in fields])
        self.only_fields = fields.keys()
        return self.fields(True, **fields)

    def exclude(self, *fields):
        """Opposite to .only(), exclude some document's fields. ::

            post = BlogPost.objects(...).exclude("comments")

        .. note :: `exclude()` is chainable and will perform a union ::
            So with the following it will exclude both: `title` and `author.name`::

                post = BlogPost.objects.exclude("title").exclude("author.name")

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: fields to exclude

        .. versionadded:: 0.5
        """
        fields = dict([(f, QueryFieldList.EXCLUDE) for f in fields])
        return self.fields(**fields)

    def order_by(self, *keys):
        """Order the :class:`~mongoengine.queryset.QuerySet` by the keys. The
        order may be specified by prepending each of the keys by a + or a -.
        Ascending order is assumed.

        :param keys: fields to order the query results by; keys may be
            prefixed with **+** or **-** to determine the ordering direction
        """
        queryset = self.clone()
        queryset._ordering = queryset._get_order_by(keys)
        return queryset

    #
    # Private methods
    #

    def _clone(self, **kwargs):
        query = self._query.clone()
        clone = self.__class__(entity=self._entity, query=query)
        clone._fields = self._fields

        clone.__dict__.update(kwargs)

        return clone

    def _fetch_all(self):
        """
        Evaluates this query set and populates the cache. Does nothing is the cache is already populated.

        Ultimately, this just ends up evaluating the underlying :class:`~gcloudoem.datastore.query.Quert` and saving
        the results returned by the :class:`~gcloudoem.datastore.query.Cursor`.
        """
        if not self._results_cache:
            self._results_cache = list(self.iterator())
