from __future__ import absolute_import

from io import open
import os

from . import entity, properties
from .datastore import credentials, connection, environment, helper, set_defaults, connect
from .entity import *
from .properties import *

version_file = open(os.path.join(os.path.dirname(__file__), 'VERSION'), encoding='utf-8')
VERSION = version_file.read().strip()
version_file.close()
