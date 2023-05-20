# treemotion/__init__.py

from .config import configuration
from .utilities.log import get_logger, configure_logger

configure_logger()
logger = get_logger(__name__)


from .utilities.database import DatabaseManager

# Erstellen Sie eine Instanz des DatabaseManagers, die im Rest Ihres Pakets verwendet wird
db_manager = DatabaseManager()
logger.debug(f"Instanze von db_manager erstellt")


from .classes.projekt import Projekt
from .classes.messreihe import Messreihe
from .classes.messung import Messung
from .classes.data import Data
from .classes.wind_messreihe import WindMessreihe

logger.debug(f"TreeMotion Package imported!")
