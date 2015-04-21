# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import

from io import open
import os

from .datastore import credentials, connection, environment, utils, connect, query
from .entity import *
from .properties import *

version_file = open(os.path.join(os.path.dirname(__file__), 'VERSION'), encoding='utf-8')
VERSION = version_file.read().strip()
version_file.close()
