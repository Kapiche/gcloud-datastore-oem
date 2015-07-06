# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from __future__ import absolute_import, division, print_function, unicode_literals


COMPARISON_OPERATORS = ('gt', 'gte', 'lt', 'lte', 'in',)
LOOKUP_SEP = '__'


def convert_lookups(**query):
    """
    Transform a query from Django-style format to Datasore format.

    :return: An iterable of filters suitable to pass to :meth:`~gcloudoem.datastore.query.Query.add_filter`.
    """
    filters = []
    for key, value in sorted(query.items()):
        parts = key.rsplit(LOOKUP_SEP)
        parts = [part for part in parts if not part.isdigit()]
        # Figure out the operator

        op = 'eq'
        if len(parts) > 1 and parts[-1] in COMPARISON_OPERATORS:
            op = parts.pop()

        # Convert to datastore notation for key. Just a nice-to-have.
        if parts[0] == 'pk':
            parts[0] = 'key'

        # Save the filter
        filters.append((parts[0], op, value,))
    return filters
