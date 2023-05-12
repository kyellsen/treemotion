# treemotion/__init__.py
print("imported treemotion")

from treemotion.config import configuration  # get classe-instance

from utilities.log import configure_logger, get_logger

configure_logger()
logger = get_logger(__name__)

from .classes.projekt import Projekt
# from .classes.messreihe import Messreihe
# from .classes.messung import Messung
# from .classes.data import Data
