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
Module to provide implicit behavior based on environment.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import socket

from six.moves.http_client import HTTPConnection

try:
    from google.appengine.api import app_identity
except ImportError:
    app_identity = None


_DATASET_ENV_VAR_NAME = 'GCLOUD_DATASET_ID'
_GCD_DATASET_ENV_VAR_NAME = 'DATASTORE_DATASET'


def compute_engine_id():
    """
    Gets the Compute Engine project ID if it can be inferred.

    Uses 169.254.169.254 for the metadata server to avoid request latency from DNS lookup.

    See https://cloud.google.com/compute/docs/metadata#metadataserver for information about this IP address. (This IP is
    also used for Amazon EC2 instances, so the metadata flavor is crucial.)

    See https://github.com/google/oauth2client/issues/93 for context about DNS latency.

    :rtype: string or ``NoneType``
    :returns: Compute Engine project ID if the metadata service is available, else ``None``.
    """
    host = '169.254.169.254'
    uri_path = '/computeMetadata/v1/project/project-id'
    headers = {'Metadata-Flavor': 'Google'}
    connection = HTTPConnection(host, timeout=0.1)

    try:
        connection.request('GET', uri_path, headers=headers)
        response = connection.getresponse()
        if response.status == 200:
            return response.read()
    except socket.error:  # socket.timeout or socket.error(64, 'Host is down')
        pass
    finally:
        connection.close()
    return None


def determine_default_dataset_id(dataset_id=None):
    """Determine default dataset ID explicitly or implicitly as fall-back.

    In implicit case, supports four environments. In order of precedence, the
    implicit environments are:

    * GCLOUD_DATASET_ID environment variable
    * DATASTORE_DATASET environment variable (for ``gcd`` testing)
    * Google App Engine application ID
    * Google Compute Engine project ID (from metadata server)

    :type dataset_id: string
    :param dataset_id: Optional. The dataset ID to use as default.

    :rtype: string or ``NoneType``
    :returns: Default dataset ID if it can be determined.
    """
    if dataset_id is None:
        dataset_id = os.getenv(_DATASET_ENV_VAR_NAME)

    if dataset_id is None:
        dataset_id = os.getenv(_GCD_DATASET_ENV_VAR_NAME)

    if dataset_id is None:
        dataset_id = compute_engine_id()

    return dataset_id
