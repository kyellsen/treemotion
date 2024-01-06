from typing import Tuple, Optional
from kj_core import DataManager
from kj_core import DatabaseManager
from kj_core import PlotManager
from kj_core import log_manager, get_logger
from .classes import DataTMS, DataLS3, DataWind, DataWindStation

from .config import Config

# from .classes import Series, Sensor, Measurement

CONFIG = None
DATA_MANAGER = None
DATABASE_MANAGER = None
PLOT_MANAGER = None


def setup(working_directory: Optional[str] = None, log_level: Optional[str] = None) -> Tuple[
    Config, DataManager, DatabaseManager, PlotManager]:
    """
    Set up the linescale3 package with specific configurations.

    Parameters:
        working_directory (str, optional): Path to the working directory.
        log_level (str, optional): Logging level.
    """
    global CONFIG, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER

    CONFIG = Config(working_directory, log_level)
    log_manager.configure_logger(CONFIG)

    name = CONFIG.package_name
    name_s = CONFIG.package_name_short

    logger = get_logger(__name__)
    logger.info(f"{name_s}: Setup {name} package!")
    logger.info(f"{name_s}: CONFIG initialized: {CONFIG}")
    logger.info(f"{name_s}: LOGGER initialized")

    DATA_MANAGER = DataManager(CONFIG)
    logger.info(f"{name_s}: DATA_MANAGER initialized: {DATA_MANAGER}")

    DATA_MANAGER.register_listeners([DataTMS, DataLS3, DataWind, DataWindStation])

    DATABASE_MANAGER = DatabaseManager(CONFIG)
    logger.info(f"{name_s}: DATABASE_MANAGER initialized: {DATABASE_MANAGER}")

    PLOT_MANAGER = PlotManager(CONFIG)
    logger.info(f"{name_s}: PLOT_MANAGER initialized: {PLOT_MANAGER}")

    logger.info(f"{name_s}: {name} setup completed.")

    return CONFIG, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER
