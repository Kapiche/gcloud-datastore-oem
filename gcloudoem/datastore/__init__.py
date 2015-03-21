from . import environment
from .environment import set_default_connection, set_default_dataset_id


def set_defaults(dataset_id=None, connection=None):
    """
    Set defaults either explicitly or implicitly as fall-back.

    Uses the arguments to call the individual default methods
    - set_default_dataset_id
    - set_default_connection

    In the future we will likely enable methods like
    - set_default_namespace

    :param str dataset_id: Optional. The dataset ID to use as default.
    :param :class:`gcloud.datastore.connection.Connection` connection: A connection provided to be the default.
    """
    set_default_dataset_id(dataset_id=dataset_id)
    set_default_connection(connection=connection)

connect = set_defaults  # Just so we are more database like
