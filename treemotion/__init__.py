# treemotion/__init__.py
print("imported treemotion")

import logging
from utilities.logging_config import configure_logger

print("logging_config.configure_logger")
configure_logger()
print("logging.getLogger")
logger = logging.getLogger(__name__)

from .classes.projekt import Projekt
from .classes.messreihe import Messreihe
from .classes.messung import Messung
from .classes.data import Data
