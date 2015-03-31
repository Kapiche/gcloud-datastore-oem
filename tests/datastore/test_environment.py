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

import os

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import unittest2

from gcloudoem.datastore.environment import _DATASET_ENV_VAR_NAME, _GCD_DATASET_ENV_VAR_NAME, \
    determine_default_dataset_id


class TestEnvironment(unittest2.TestCase):
    def test_default_default_env(self):
        DATASET = object()
        fake_environ = {_DATASET_ENV_VAR_NAME: DATASET}

        with patch.object(os, 'getenv', fake_environ.get):
            self.assertEqual(determine_default_dataset_id(), DATASET)

    def test_default_gcd_env(self):
        DATASET = object()
        fake_environ = {_GCD_DATASET_ENV_VAR_NAME: DATASET}

        with patch.object(os, 'getenv', fake_environ.get):
            self.assertEqual(determine_default_dataset_id(), DATASET)

    def test_default_compute_env(self):
        self.assertIsNone(determine_default_dataset_id())
