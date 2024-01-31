from typing import Tuple, Optional, Any
from kj_logger import get_logger, LogManager, LOG_MANAGER

from .config import Config
from kj_core import DataManager
from kj_core import DatabaseManager
from kj_core import PlotManager

from .classes import DataWindStation, DataTMS, DataMerge, DataLS3
from .classes import Measurement, Series, Project, MeasurementVersion

CONFIG = None
DATA_MANAGER = None
DATABASE_MANAGER = None
PLOT_MANAGER = None


def setup(working_directory: Optional[str] = None, log_level="info", safe_logs_to_file=True) -> tuple[Config, LogManager, DataManager, DatabaseManager, PlotManager]:
    """
    Set up the treemotion package with specific configurations.

    Parameters:
        working_directory (str, optional): Path to the working directory.
        log_level (str, optional): Logging level.
        safe_logs_to_file
    """
    global CONFIG, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER

    LOG_MANAGER.update_config(working_directory, log_level, safe_logs_to_file)

    logger = get_logger(__name__)

    CONFIG = Config(working_directory)

    name = CONFIG.package_name
    name_s = CONFIG.package_name_short

    logger.info(f"{name_s}: Setup {name} package!")
    DATA_MANAGER = DataManager(CONFIG)

    # Listen to changes on Attribut-"data" for all classes of type CoreDataClass
    DATA_MANAGER.register_listeners([DataWindStation, DataTMS, DataMerge, DataLS3])

    DATABASE_MANAGER = DatabaseManager(CONFIG)

    PLOT_MANAGER = PlotManager(CONFIG)

    logger.info(f"{name_s}: {name} setup completed.")

    return CONFIG, LOG_MANAGER, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER
