# treemotion/utilities/db_manager.py
from pathlib import Path
from shutil import copy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from contextlib import contextmanager
from .base import Base
from .log import get_logger
from config import configuration

logger = get_logger(__name__)


class DatabaseManager:
    """Manager for database connections."""

    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        if self._instance is not None:
            raise ValueError("An instantiation already exists!")
        self._engines = {}
        self._sessions = {}
        self._default_db_name = None
        logger.info('DatabaseManager initialized')

    def connect_db(self, db_path, echo=False, set_as_default=True):
        """Initializes the connection to a database."""
        db_path = Path(db_path)
        if not db_path.is_file():
            logger.error(f'Database file {db_path} does not exist')
            raise FileNotFoundError(f'Database file {db_path} does not exist')
        db_name = db_path.stem  # The name of the database is the filename without extension
        try:
            engine = create_engine(f"sqlite:///{db_path}", echo=echo)
            Base.metadata.create_all(engine)
            self._engines[db_name] = engine
            self._sessions[db_name] = scoped_session(sessionmaker(bind=engine))

            # Set the passed database as default if 'set_as_default' flag is set
            if set_as_default:
                self._default_db_name = db_name

            logger.info(f'Database {db_name} connected')
        except Exception as e:
            logger.error(f'Error connecting to database {db_name}: {e}')
            raise

    def disconnect_db(self, db_name):
        """Deletes the connection to a database."""
        if db_name in self._sessions:
            try:
                self._sessions[db_name].remove()
                del self._sessions[db_name]
                del self._engines[db_name]

                if self._default_db_name == db_name:
                    self._default_db_name = None

                logger.info(f'Database {db_name} disconnected')
            except Exception as e:
                logger.error(f'Error disconnecting from database {db_name}: {e}')
                raise
        else:
            logger.warning(f'Attempted to disconnect from non-existent database {db_name}')

    def get_session(self, db_name=None):
        """Returns the current get_session for a database."""
        try:
            if db_name is None:
                if self._default_db_name is None:
                    logger.error(
                        "No default database connection set. Please initialize a database with 'set_as_default=True' first.")
                    return None
                db_name = self._default_db_name
                logger.debug(f"_default_db_name: {self._default_db_name}")
            if db_name not in self._sessions:
                logger.error(f"Database connection '{db_name}' is not initialized. Please call 'connect_db' first.")
                return None
            # logger.debug(f"self._sessions[db_name] {self._sessions[db_name]}")
            return self._sessions[db_name]()
        except Exception as e:
            logger.error(f'Error getting get_session for database {db_name}: {e}')
            raise

    def close_session(self, db_name=None):
        """Closes the current get_session for a database."""
        try:
            if db_name is None:
                if self._default_db_name is None:
                    logger.error(
                        "No default database connection set. Please initialize a database with 'set_as_default=True' first.")
                    return
                db_name = self._default_db_name

            if db_name not in self._sessions:
                logger.error(f"Database connection '{db_name}'does not exist.")
                return
            self._sessions[db_name].remove()
            logger.info(f'Session for database {db_name} closed')
        except Exception as e:
            logger.error(f'Error closing get_session for database {db_name}: {e}')
            raise

    @contextmanager
    def get_session_scope(self, db_name=None):
        """Provides a transactional scope around a series of operations."""
        session = self.get_session(db_name)
        try:
            yield session
            session.commit()
            logger.info(f'Transaction for database {db_name} committed')
        except Exception as e:
            session.rollback()
            logger.error(f'Error in transaction for database {db_name}: {e}. Transaction rolled back.')
            raise
        finally:
            session.close()
            logger.info(f'Session for database {db_name} closed')

    def create_new_db(cls, path, name):
        path = Path(path)
        filename = f"{name}.db"
        path_db = path / filename

        if path_db.exists():
            logger.error(f"Error: A database named '{filename}' already exists in {path}.")
            return None

        template_db_name = configuration.template_db_name
        current_file = Path(__file__)
        parent_directory = current_file.parent.parent
        template_db_path = parent_directory / template_db_name

        if not template_db_path.exists() or not template_db_path.is_file():
            logger.error(f"Error: The {template_db_name} file was not found.")
            return None

        try:
            copy(template_db_path, path_db)
            logger.info(f"Database '{filename}' was successfully created in {path}.")
            return str(path_db)
        except Exception as e:
            logger.error(f"Error creating the database {filename}: {e}")
            return None
