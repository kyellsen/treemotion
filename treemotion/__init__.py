# treemotion/__init__.py

from .config import Configuration
from .utilities.log import get_logger, configure_logger
from .utilities.database import DatabaseManager

# Eine Instanz der Configuration-Klasse erstellen
configuration = Configuration()

# Eine Instanz der Logger-Klasse erstellen
configure_logger()
logger = get_logger(__name__)

logger.debug(f"Importiere TreeMotion Package!")
logger.debug(f"Config-Instanz und Logger erstellt!")

# Erstellen Sie eine Instanz des DatabaseManagers, die im Rest Ihres Pakets verwendet wird
db_manager = DatabaseManager()
logger.debug(f"Datenbank Manager erstellt!")

from .classes.projekt import Projekt
from .classes.messreihe import Messreihe
from .classes.messung import Messung
from .classes.data import Data
from .classes.wind_messreihe import WindMessreihe

from .plotting.plot_manager import PlotManager

logger.info(f"TreeMotion Package importiert!")
