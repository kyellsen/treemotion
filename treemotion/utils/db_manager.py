# treemotion/utils/db_manager.py

from pathlib import Path
from typing import Optional
from shutil import copy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from utils.log import get_logger
from utils.base import Base

logger = get_logger(__name__)


class DatabaseManager:
    """
    A class for managing database connections and sessions.
    """

    def __init__(self):
        """
        Constructor of the DatabaseManager class.
        Initializes the engine and session_factory attributes to None.
        """
        self.engine = None
        self.session_factory = None
        self.current_session = None

    def connect(self, db_filename: str, directory: str = None):
        """
        Connects to the database and creates a new session_factory.

        Args:
            db_filename (str): Path to the database.
            directory (str): Optional, usually in the working_directory.
        """
        if directory is None:
            from treemotion import config
            directory = config.working_directory
        else:
            directory = Path(directory)

        directory.mkdir(parents=True, exist_ok=True)
        db_path = directory / db_filename  # Create DB path from working directory and filename

        DATABASE_URI = f'sqlite:///{db_path.__str__()}'

        # Check if the database already exists
        if db_path.is_file():
            logger.info(f"Using existing database at {db_path.__str__()}.")
        else:
            logger.info(f"Database at {db_path.__str__()} does not exist and will be created.")

        try:
            self.engine = create_engine(DATABASE_URI, connect_args={'timeout': 30})
            Base.metadata.create_all(self.engine)  # Create all tables defined in your Base ORM object
            self.session_factory = sessionmaker(bind=self.engine, autocommit=False)
            self.current_session = self.session_factory()
            logger.info(f"Successfully connected to the database at {db_path.__str__()}.")
        except Exception as e:
            logger.error(f"Error while connecting to the database: {e}")
            raise e

    def disconnect(self):
        """
        Disconnects from the database.
        """
        if self.engine is not None:
            if self.current_session is not None:
                if len(self.current_session.dirty) > 0 or len(self.current_session.new) > 0:
                    logger.warning("There are uncommitted changes.")
                    self.ask_commit(self.current_session)
                self.current_session.close()
                self.current_session = None
            self.session_factory = None
            self.engine.dispose()
            self.engine = None
            logger.info("Successfully disconnected from the database.")
        else:
            logger.warning("There is no active database connection to disconnect.")

    def get_session(self, session=None):
        """
        Returns the specified session or, if not specified, the current session.

        Args:
            session (Optional[Session]): An optional session.

        Returns:
            Session: The specified or current session.
        """
        if self.session_factory is None:
            logger.error("Database is not connected. Please call connect() first.")
            raise Exception("Database is not connected. Please call connect() first.")

        if session is None:
            return self.current_session
        else:
            return session

    def commit(self, session: Optional[Session] = None, class_name: Optional[str] = None,
               method_name: Optional[str] = None) -> bool:
        """
        Commits all changes in the specified session to the database.
        Automatically logs the result.

        Args:
            session (Session, optional): The session in which the changes were made.
                                          If None, a new session is created. Defaults to None.
            class_name (str, optional): Name of the class in which the method was called. Defaults to None.
            method_name (str, optional): Name of the method in which changes were made. Defaults to None.

        Returns:
            bool: True if commit was successful, False otherwise.
        """
        session = session or self.get_session()
        try:
            session.commit()
            if class_name is not None and method_name is not None:
                logger.debug(f"Commit successful for method '{method_name}' in class '{class_name}'.")
            else:
                logger.debug(f"Commit successful.")
            return True
        except Exception as e:
            session.rollback()
            if class_name is not None and method_name is not None:
                logger.error(f"Commit failed for method '{method_name}' in class '{class_name}'.")
            else:
                logger.error(f"Error while committing to the database: {e}")
            return False

    def open_session(self):
        """
        Creates and returns a new session.

        Returns:
            Session: A new session.
        """
        if self.session_factory is None:
            logger.error("Database is not connected. Please call connect() first.")
            raise Exception("Database is not connected. Please call connect() first.")
        self.current_session = self.session_factory()
        logger.debug("Successfully created a new session.")
        return self.current_session

    def close_session(self, session):
        """
        Closes the specified session.

        Args:
            session (Session): The session to close.
        """
        try:
            if len(session.dirty) > 0 or len(session.new) > 0:
                logger.warning("There are uncommitted changes.")
                self.ask_commit(session)
            session.close()
            if session == self.current_session:
                self.current_session = None
            logger.debug("Successfully closed the session.")
        except Exception as e:
            logger.error("Error while closing the session: ", e)
            raise e

    @staticmethod
    def create_template_db(name: str, path: str = None):
        """
        Creates a new database by copying the template database.

        Args:
            name (str): The name of the new database.
            path (str): The path where the new database should be created.

        Returns:
            str: The path to the new database or None if the database could not be created.
        """
        if path is None:
            from treemotion import config
            path = config.working_directory

        path = Path(path)
        database_filename = f"{name}.db"
        database_path = path / database_filename

        if database_path.exists():
            logger.error(f"A database named '{database_filename}' already exists in {path}.")
            return str(database_path)

        template_database_filename = config.template_db_name
        template_database_path = Path(__file__).parent.parent / "resources" / template_database_filename

        if not template_database_path.is_file():
            logger.error(f"The file {template_database_filename} was not found.")
            return

        try:
            copy(template_database_path, database_path)
            logger.info(f"Database '{database_filename}' created successfully in {path}.")
            return str(database_path)
        except Exception as e:
            logger.error(f"Error while creating the database {database_filename}: ", e)
            return None

    @staticmethod
    def ask_commit(session):
        """
        Asks the user if they want to commit the changes.

        Args:
            session (Session): The session in which the changes were made.
        """
        # START GUI MODIFICATION
        commit = input("Do you want to commit? (True/False): ")
        # END GUI MODIFICATION
        if commit.lower() == 'true':
            try:
                session.commit()
                logger.info("Changes committed successfully to the database.")
            except Exception as e:
                session.rollback()
                logger.error("Error while committing to the database: ", e)
                raise e
        else:
            logger.warning("Changes were not committed to the database and discarded.")
