# treemotion/__init__.py
print("imported treemotion")

from .config import configuration
from .utilities.log import get_logger, configure_logger

configure_logger()
logger = get_logger(__name__)

from .utilities.database import DatabaseManager

# Erstellen Sie eine Instanz des DatabaseManagers, die im Rest Ihres Pakets verwendet wird
db_manager = DatabaseManager()


from .classes.projekt import Projekt
from .classes.messreihe import Messreihe
from .classes.messung import Messung
from .classes.data import Data

