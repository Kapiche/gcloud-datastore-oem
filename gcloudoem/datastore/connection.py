# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# CHANGED BY Kapiche Ltd.
# Copyright 2015 Kapiche Ltd. All rights reserved.
# Based on work by the good folk responsible for gcloud-python. Thanks folks!
# Author: Ryan Stuart<ryan@kapiche.com>
#
"""
Connections to gcloud Datastore API servers.

This module also handles connections via :func:`.register_connection` and :func:`.get_connection`. Right now, this isn't
used internally but could be later on.
"""
from __future__ import absolute_import, division, print_function

import os
import random
import time

from .base import BaseConnection
from ._generated import datastore_pb2 as datastore_pb
from ..exceptions import ConnectionError, make_exception


DEFAULT_NAMESPACE = 'default'

DATASTORE_API_HOST = 'datastore.googleapis.com'
"""Datastore API request host."""
GCD_HOST = 'DATASTORE_EMULATOR_HOST'
"""Environment variable defining host for GCD dataset server."""

_connections = {}
_default_connection = None


def register_connection(dataset, namespace, credentials):
    """Shortcut to create a new connection."""
    global _connections, _default_connection

    connection = Connection(dataset, namespace, credentials)
    _connections[namespace] = connection
    _default_connection = connection


def get_connection(alias=None):
    global _connections, _default_connection

    if alias is None:
        if _default_connection is None:
            raise ConnectionError('There is no active connection.')
        return _default_connection

    try:
        return _connections[alias]
    except KeyError:
        raise ConnectionError('Connection with alias "%s" has not been defined' % alias)


def disconnect():
    """Remove all connections."""
    global _connections, _default_connection

    _connections.clear()
    _default_connection = None


class Connection(BaseConnection):
    """
    A connection to the Google Cloud Datastore via the Protobuf API.

    A connection in datastore terms is a combination of gcloud credentials and a dataset id (project id). While its
    possible to use the same same credentials across projects in gcloud, it's rarely done and keeping all these things
    together aligns much better with a traditional database.
    """
    API_BASE_URL = 'https://' + DATASTORE_API_HOST
    """The base of the API call URL."""

    API_VERSION = 'v1'
    """The version of the API, used in building the API call's URL."""

    # API_URL_TEMPLATE = ('{api_base}/datastore/{api_version}/datasets/{dataset_id}/{method}')
    API_URL_TEMPLATE = ('{api_base}/{api_version}/projects/{dataset_id}:{method}')
    """A template for the URL of a particular API call."""

    RETRY_STATUSES = [500, 502, 503, 504]
    """Automatically retry a request when we encounter any of these status codes."""

    MAX_RETRIES = 2
    """Number of times to try a request when a RETRY_STATUSES code is encountered."""

    def __init__(self, dataset_id, namespace, credentials=None, http=None, api_base_url=None):
        """
        :param str dataset_id: The gcloud Datastore dataset identified.
        :param str namespace: The gcloud Datastore namesapce to use.
        :param :class:`oauth2client.client.OAuth2Credentials` credentials: The OAuth2 Credentials to use for this
            connection.
        :param http: A class to use instead of :class:`httplib2.Http` for calling the API. Needs to have a ``request``
            method that accepts the following arguments: ``uri``, ``method``, ``body`` and ``headers``.
        :param str api_base_url: The base of the API call URL. Defaults to
            :attr:`~gcloudoem.datastore.base.BaseConnection.API_BASE_URL`.
        """
        super(Connection, self).__init__(dataset_id, namespace, credentials=credentials, http=http)
        try:
            self.host = os.environ[GCD_HOST]
            self.api_base_url = 'http://' + self.host
        except KeyError:
            self.host = DATASTORE_API_HOST
            self.api_base_url = self.__class__.API_BASE_URL

    def _request(self, method, data):
        """Make a request over the Http transport to the Cloud Datastore API.

        :param str method: The API call method name (ie, ``runQuery``, ``lookup``, etc)

        :param str data: The data to send with the API call. Typically this is a serialized Protobuf string.

        :rtype: str
        :returns: The str response content from the API call.
        :raises: :class:`~gcloudoem.exceptions.GCloudError` if the response code is not 200 OK.
        """
        retries = 0
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Content-Length': str(len(data)),
            'User-Agent': self.USER_AGENT,
        }
        while True:
            #try:
            headers, content = self.http.request(
                uri=self.build_api_url(method=method),
                method='POST',
                headers=headers,
                body=data
            )
            #except ssl.SSLError as e:
                #import pdb
                #pdb.set_trace()

            status = headers['status']
            if status != '200':
                if status in self.RETRY_STATUSES and retries < self.MAX_RETRIES:
                    retries += 1
                    time.sleep(random.random() * retries)
                    continue
                raise make_exception(headers, content, use_json=False)
            return content

    def _rpc(self, method, request_pb, response_pb_cls):
        """
        Make a protobuf RPC request.

        :param str method: The name of the method to invoke.

        :param :class:`~google.protobuf.message.Message` request_pb: the protobuf instance representing the request.

        :param :class:`google.protobuf.message.Message` response_pb_cls: The class used to unmarshall the response
            protobuf.
        """
        response = self._request(
            method=method,
            data=request_pb.SerializeToString()
        )
        return response_pb_cls.FromString(response)

    def build_api_url(self, method, base_url=None, api_version=None):
        """Construct the URL for a particular API call.

        This method is used internally to come up with the URL to use when making RPCs to the Cloud Datastore API.

        :type method: string
        :param method: The API method to call (ie, runQuery, lookup, ...).

        :type base_url: string
        :param base_url: The base URL where the API lives. You shouldn't have to provide this.

        :type api_version: string
        :param api_version: The version of the API to connect to. You shouldn't have to provide this.
        """
        return self.API_URL_TEMPLATE.format(
            api_base=(base_url or self.api_base_url),
            api_version=(api_version or self.API_VERSION),
            dataset_id=self.dataset,
            method=method,
        )

    def lookup(self, key_pbs, eventual=False, transaction_id=None):
        """
        Lookup keys from in the Cloud Datastore.

        Maps the ``DatastoreService.Lookup`` protobuf RPC.

        This method deals only with protobufs (:class:`gcloud.datastore._datastore_v1_pb2.Key` and
        :class:`gcloud.datastore._datastore_v1_pb2.Entity`) and is used under the hood.

        :param list key_pbs: The keys to retrieve from the datastore.

        :param bool eventual: If False (the default), request ``STRONG`` read consistency.  If True, request
            ``EVENTUAL`` read consistency.

        :param str transaction_id: If passed, make the request in the scope of the given transaction.  Incompatible with
            ``eventual==True``.

        :rtype: tuple
        :returns: A triple of (``results``, ``missing``, ``deferred``) where both ``results`` and ``missing`` are lists
            of :class:`gcloud.datastore._datastore_v1_pb2.Entity` and ``deferred`` is a list of
            :class:`gcloud.datastore._datastore_v1_pb2.Key`.
        """
        lookup_request = datastore_pb.LookupRequest()
        _set_read_options(lookup_request, eventual, transaction_id)
        _add_keys_to_request(lookup_request.key, key_pbs)

        lookup_response = self._rpc('lookup', lookup_request, datastore_pb.LookupResponse)

        results = [result.entity for result in lookup_response.found]
        missing = [result.entity for result in lookup_response.missing]

        return results, missing, list(lookup_response.deferred)

    def run_query(self, query_pb, namespace=None, eventual=False, transaction_id=None):
        """Run a query on the Cloud Datastore.

        Maps the ``DatastoreService.RunQuery`` protobuf RPC.

        Given a Query protobuf, sends a ``runQuery`` request to the Cloud Datastore API and returns a list of entity
        protobufs matching the query.

        You typically wouldn't use this method directly, in favor of the
        :meth:`~gcloudoem.datastore.query.Query.execute` method.

        :type query_pb: :class:`gcloudoem.datastore.datastore_v1_pb2.Query`
        :param query_pb: The Protobuf representing the query to run.

        :type namespace: string
        :param namespace: The namespace over which to run the query.

        :type eventual: boolean
        :param eventual: If False (the default), request ``STRONG`` read consistency.  If True, request ``EVENTUAL``
            read consistency.

        :type transaction_id: string
        :param transaction_id: If passed, make the request in the scope of the given transaction.  Incompatible with
            ``eventual==True``.
        """
        request = datastore_pb.RunQueryRequest()
        _set_read_options(request, eventual, transaction_id)

        if namespace:
            request.partition_id.namespace_id = namespace

        request.query.CopyFrom(query_pb)
        response = self._rpc('runQuery', request, datastore_pb.RunQueryResponse)
        return (
            [e.entity for e in response.batch.entity_results],
            response.batch.end_cursor,  # Assume response always has cursor.
            response.batch.more_results,
            response.batch.skipped_results,
        )

    def begin_transaction(self, serializable=False):
        """
        Begin a transaction.

        Maps the ``DatastoreService.BeginTransaction`` protobuf RPC.

        :param bool serializable: Boolean indicating if the isolation level of the transaction should be SERIALIZABLE
            (True) or SNAPSHOT (False).

        :rtype: :class:`.datastore_v1_pb2.BeginTransactionResponse`
        :returns': the result protobuf for the begin transaction request.
        """
        request = datastore_pb.BeginTransactionRequest()

        # if serializable:
        #     request.isolation_level = (datastore_pb.BeginTransactionRequest.SERIALIZABLE)
        # else:
        #     request.isolation_level = (datastore_pb.BeginTransactionRequest.SNAPSHOT)

        response = self._rpc('beginTransaction', request, datastore_pb.BeginTransactionResponse)

        return response.transaction

    def commit(self, request, transaction_id=None):
        """
        Commit dataset mutations in context of current transation (if any).

        Maps the ``DatastoreService.Commit`` protobuf RPC.

        :param :class:`._datastore_v1_pb2.Mutation` mutation_pb: The protobuf for the mutations being saved.

        :param str transaction_id: The transaction ID returned from :meth:`begin_transaction`.  If not passed, the
            commit will be non-transactional.

        :rtype: :class:`._datastore_v1_pb2.MutationResult`.
        :returns': the result protobuf for the mutation.
        """
        # request = datastore_pb.CommitRequest()

        if transaction_id:
            request.mode = datastore_pb.CommitRequest.TRANSACTIONAL
            request.transaction = transaction_id
        else:
            request.mode = datastore_pb.CommitRequest.NON_TRANSACTIONAL

        # request.mutation.CopyFrom(mutation_pb)
        return self._rpc('commit', request, datastore_pb.CommitResponse)

    def rollback(self, transaction_id):
        """
        Rollback the connection's existing transaction.

        Maps the ``DatastoreService.Rollback`` protobuf RPC.

        :param str transaction_id: The transaction ID returned from :meth:`begin_transaction`.
        """
        request = datastore_pb.RollbackRequest()
        request.transaction = transaction_id
        # Nothing to do with this response, so just execute the method.
        self._rpc('rollback', request, datastore_pb.RollbackResponse)

    def allocate_ids(self, key_pbs):
        """
        Obtain backend-generated IDs for a set of keys.

        Maps the ``DatastoreService.AllocateIds`` protobuf RPC.

        :type key_pbs: list of
        :param list key_pbs: The :class:`~._datastore_v1_pb2.Key`s for which the backend should
            allocate IDs.

        :rtype: list of :class:`.datastore_v1_pb2.Key`
        :returns: An equal number of keys,  with IDs filled in by the backend.
        """
        request = datastore_pb.AllocateIdsRequest()
        _add_keys_to_request(request.key, key_pbs)
        # Nothing to do with this response, so just execute the method.
        response = self._rpc('allocateIds', request, datastore_pb.AllocateIdsResponse)
        return list(response.key)


def _set_read_options(request, eventual, transaction_id):
    """
    Validate rules for read options, and assign to the request.

    Helper method for ``lookup()`` and ``run_query``.

    :raises: :class:`ValueError` if ``eventual`` is ``True`` and the ``transaction_id`` is not ``None``.
    """
    if eventual and (transaction_id is not None):
        raise ValueError('eventual must be False when in a transaction')

    opts = request.read_options
    if eventual:
        opts.read_consistency = datastore_pb.ReadOptions.EVENTUAL
    elif transaction_id:
        opts.transaction = transaction_id


def _add_keys_to_request(request_field_pb, key_pbs):
    """
    Add protobuf keys to a request object.

    :type request_field_pb: `RepeatedCompositeFieldContainer`
    :param request_field_pb: A repeated proto field that contains keys.

    :type key_pbs: of :
    :param list key_pbs: The class:`~gcloudoem.datastore.datastore_v1_pb2.Key`s to add to a request.
    """
    for key_pb in key_pbs:
        request_field_pb.add().CopyFrom(key_pb)
