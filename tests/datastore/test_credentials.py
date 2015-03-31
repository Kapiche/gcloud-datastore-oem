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
# Copyright (C) 2015 Kapiche Ltd. All rights reserved.
# Based on work by the good folk responsible for gcloud-python. Thanks folks!
# Author: Ryan Stuart<ryan@kapiche.com>
#
from __future__ import absolute_import, division, print_function, unicode_literals

import base64

from six.moves.urllib.parse import parse_qs
from six.moves.urllib.parse import urlsplit
import unittest2

from gcloudoem.datastore import credentials

try:
    from unittest.mock import patch, Mock, sentinel, call, DEFAULT
except ImportError:
    from mock import patch, Mock


class TestCredentials(unittest2.TestCase):
    @patch('gcloudoem.datastore.credentials.client.GoogleCredentials.get_application_default')
    def test_get_credentials(self, mock):
        credentials.get_credentials()
        mock.assert_called_with()

    @patch('gcloudoem.datastore.credentials.client.SignedJwtAssertionCredentials', spec=True)
    def test_get_for_service_account_p12_wo_scope(self, mock):
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile() as file_obj:
            file_obj.write(b'DATA')
            file_obj.flush()
            credentials.get_for_service_account_p12(sentinel.email, file_obj.name)
        mock.assert_called_with(service_account_name=sentinel.email, private_key=b'DATA', scope=None)

    @patch('gcloudoem.datastore.credentials.client.SignedJwtAssertionCredentials', spec=True)
    def test_get_for_service_account_p12_w_scope(self, mock):
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile() as file_obj:
            file_obj.write(b'DATA')
            file_obj.flush()
            credentials.get_for_service_account_p12(sentinel.email, file_obj.name, sentinel.scope)
        mock.assert_called_with(service_account_name=sentinel.email, private_key=b'DATA', scope=sentinel.scope)

    @patch('gcloudoem.datastore.credentials._get_application_default_credential_from_file')
    def test_get_for_service_account_json_wo_scope(self, mock):
        credentials.get_for_service_account_json(sentinel.file)
        mock.assert_called_with(sentinel.file)

    @patch('gcloudoem.datastore.credentials._get_application_default_credential_from_file')
    def test_get_for_service_account_jsont_w_scope(self, mock):
        from gcloudoem.datastore import credentials

        credentials.get_for_service_account_json(sentinel.file, scope=sentinel.scope)
        self.assertEqual(mock.mock_calls, [
            call(sentinel.file),
            call().create_scoped(sentinel.scope)
        ])

    def test_generate_signed_url_w_expiration_int(self):
        ENDPOINT = 'http://api.example.com'
        RESOURCE = '/name/path'
        EXPIRATION = 1000
        SIGNED = base64.b64encode(b'DEADBEEF')

        with patch(
            'gcloudoem.datastore.credentials._get_signed_query_params',
            return_value={'GoogleAccessId': sentinel.account, 'Expires': EXPIRATION, 'Signature': SIGNED}
        ):
            url = credentials.generate_signed_url(sentinel.account, RESOURCE, 1000, api_access_endpoint=ENDPOINT)

        scheme, netloc, path, qs, frag = urlsplit(url)
        self.assertEqual(scheme, 'http')
        self.assertEqual(netloc, 'api.example.com')
        self.assertEqual(path, RESOURCE)
        params = parse_qs(qs)
        self.assertEqual(len(params), 3)
        self.assertEqual(params['Signature'], [SIGNED.decode('ascii')])
        self.assertEqual(params['Expires'], ['1000'])
        self.assertEqual(params['GoogleAccessId'], ['sentinel.account'])
        self.assertEqual(frag, '')

    @unittest2.expectedFailure
    def test_get_signed_query_params(self):
        from oauth2client import client

        EXPIRATION = 1000
        SIGNATURE = 'abc'
        scopes = []
        ACCOUNT_NAME = 'dummy_service_account_name'
        creds = client.SignedJwtAssertionCredentials(ACCOUNT_NAME, b'dummy_private_key_text', scopes)

        with patch.multiple(
            'gcloudoem.datastore.credentials', SHA256=DEFAULT, RSA=DEFAULT, PKCS1_v1_5=DEFAULT, crypt=DEFAULT
        ) as mocks:
            credentials._get_signed_query_params(creds, EXPIRATION, SIGNATURE)
            self.assertEqual(mocks['PKCS1_v1_5'].mock_calls, [call.new()])
