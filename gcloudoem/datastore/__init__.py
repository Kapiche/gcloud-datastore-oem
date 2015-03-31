# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import, division, print_function, unicode_literals

from .connection import ConnectionError, Connection, register_connection, DEFAULT_CONNECTION_NAME
from .environment import determine_default_dataset_id
from gcloudoem.datastore import credentials


SCOPE = ('https://www.googleapis.com/auth/datastore', 'https://www.googleapis.com/auth/userinfo.email')
"""The scopes required for authenticating as a Cloud Datastore consumer."""


def connect(dataset_id=None):
    """
    Connect to Datastore dataset. If no dataset is given, we attempt to determine it given the environment.

    :param str dataset_id: Optional. The dataset ID to use as default.
    """
    if dataset_id is None:
        dataset_id = determine_default_dataset_id()
        if dataset_id is None:
            raise ConnectionError("Couldn't determine the dataset id from the environment")
    implicit_credentials = credentials.get_credentials()
    scoped_credentials = implicit_credentials.create_scoped(SCOPE)
    register_connection(DEFAULT_CONNECTION_NAME, dataset_id, scoped_credentials)
