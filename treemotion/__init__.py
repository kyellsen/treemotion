# treemotion/__init__.py

from .configuration import Configuration
from .utils.log import get_logger, configure_logger
from .utils.db_manager import DatabaseManager
from .plotting.plot_manager import PlotManager

# Eine Instanz der Configuration-Klasse erstellen
config = Configuration()

# Eine Instanz der Logger-Klasse erstellen
configure_logger()
logger = get_logger(__name__)

logger.debug(f"Importiere TreeMotion Package!")
logger.debug(f"Config-Instanz und Logger erstellt!")

# Erstellen Sie eine Instanz des DatabaseManagers, die im Rest Ihres Pakets verwendet wird.
db_manager = DatabaseManager()
logger.debug(f"Datenbank Manager erstellt!")

# Erstellen Sie eine Instanz des PlotManagers, die im Rest Ihres Pakets verwendet wird.
plot_manager = DatabaseManager()
logger.debug(f"Plot Manager erstellt!")

from .classes.projekt import Projekt
from .classes.messreihe import Messreihe
from .classes.messung import Messung
from .classes.data import Data
from .classes.wind_messreihe import WindMessreihe


logger.info(f"TreeMotion Package importiert!")
