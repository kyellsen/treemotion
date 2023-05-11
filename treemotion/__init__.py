# treemotion/__init__.py
import logging

print("imported treemotion")
from utilities.log import configure_logger, get_logger

configure_logger(log_level="debug")
logger = get_logger(__name__)


from .classes.projekt import Projekt
#from .classes.messreihe import Messreihe
#from .classes.messung import Messung
#from .classes.data import Data
