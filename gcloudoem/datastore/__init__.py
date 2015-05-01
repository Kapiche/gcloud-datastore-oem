# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import, division, print_function, unicode_literals

from .connection import ConnectionError, Connection, register_connection, DEFAULT_NAMESPACE
from .environment import determine_default_dataset_id
from gcloudoem.datastore import credentials


SCOPE = ('https://www.googleapis.com/auth/datastore', 'https://www.googleapis.com/auth/userinfo.email')
"""The scopes required for authenticating as a Cloud Datastore consumer."""


def connect(dataset_id=None, namespace=DEFAULT_NAMESPACE):
    """
    Connect to Datastore. If no dataset is given, we attempt to determine it given the environment.

    :param namespace: The namespace to use. A namespace is used for multi-tenancy on datastore. It's useful for
        separating dev data from production data for example.
    :param str dataset_id: Optional. The dataset ID to use as default.
    """
    if dataset_id is None:
        dataset_id = determine_default_dataset_id()
        if dataset_id is None:
            raise ConnectionError("Couldn't determine the dataset id from the environment")
    implicit_credentials = credentials.get_credentials()
    scoped_credentials = implicit_credentials.create_scoped(SCOPE)
    register_connection(dataset_id, namespace, scoped_credentials)
