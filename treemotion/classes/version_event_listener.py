# treemotion/classes/version_event_listener.py

from sqlalchemy import event
from utils.log import get_logger
from .version import Version

logger = get_logger(__name__)


# The function to be called after deleting a Version instance
@event.listens_for(Version, 'after_delete')
def listen_to_version_delete_and_delete_table(_, __, target: Version) -> None:
    """
    Listens to the 'before_delete' event of the Version class and deletes the TMS table.

    Args:
        _: Unused mapper parameter.
        __: Unused connection parameter.
        target (Version): The Version instance being deleted.
    """
    target.delete_tms_table()

