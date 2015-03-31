# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
import re


VERSION_PICKLE_KEY = '_gcloudoem_version'
re_camel_case = re.compile(r'(((?<=[a-z])[A-Z])|([A-Z](?![A-Z]|$)))')


def camel_case_to_spaces(value):
    """Splits CamelCase and converts to lower case. Also strips leading and trailing whitespace."""
    return re_camel_case.sub(r' \1', value).strip().lower()
