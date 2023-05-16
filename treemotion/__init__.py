# treemotion/__init__.py
print("imported treemotion")

from .config import configuration
from .utilities.log import get_logger, configure_logger

configure_logger()
logger = get_logger(__name__)

from .utilities.db_manager import DatabaseManager
db_manager = DatabaseManager.get_instance()

from .classes.projekt import Projekt
from .classes.messreihe import Messreihe
from .classes.messung import Messung
from .classes.data import Data
