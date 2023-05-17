# treemotion/utilities/database.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from pathlib import Path
from shutil import copy



from config import configuration
from utilities.log import get_logger
from utilities.base import Base

logger = get_logger(__name__)


class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.session = None

    def connect(self, db_path):
        DATABASE_URI = f'sqlite:///{db_path}'
        try:
            self.engine = create_engine(DATABASE_URI)
            Base.metadata.create_all(self.engine)  # Erstellt alle Tabellen, definiert in Ihrem Base ORM-Objekt
            self.session = scoped_session(sessionmaker(bind=self.engine))
            logger.info(f"Verbindung zur Datenbank unter {DATABASE_URI} erfolgreich hergestellt.")
        except Exception as e:
            logger.error("Fehler beim Verbinden zur Datenbank.")
            raise

    def get_session(self, session=None):
        if self.session is None:
            logger.error("Datenbank ist nicht verbunden. Bitte rufen Sie zuerst connect_db() auf.")
            raise Exception("Datenbank ist nicht verbunden. Bitte rufen Sie zuerst connect_db() auf.")
        logger.debug("Session erfolgreich abgerufen.")
        return self.session if session is None else session

    def disconnect(self):
        try:
            self.session.remove()
            self.session = None
            self.engine = None
            logger.info("Datenbankverbindung erfolgreich getrennt.")
        except Exception as e:
            logger.error("Fehler beim Trennen der Datenbankverbindung.")
            raise

    def commit(self, Session=None):
        session = self.get_session(Session)
        try:
            session.commit()
            logger.debug("Änderungen erfolgreich zur Datenbank committet.")
        except Exception as e:
            logger.error("Fehler beim Commit zur Datenbank.")
            raise
    @staticmethod
    def create_db(path, name):
        path = Path(path)
        database_filename = f"{name}.db"
        database_path = path / database_filename

        if database_path.exists():
            logger.error(f"Error: A database named '{database_filename}' already exists in {path}.")
            return None

        template_database_filename = configuration.template_db_name
        current_file = Path(__file__)
        parent_directory = current_file.parent.parent
        template_database_path = parent_directory / template_database_filename

        if not template_database_path.exists() or not template_database_path.is_file():
            logger.error(f"Error: The {template_database_filename} file was not found.")
            return None

        try:
            copy(template_database_path, database_path)
            logger.info(f"Database '{database_filename}' was successfully created in {path}.")
            return str(database_path)
        except Exception as e:
            logger.error(f"Error creating the database {database_filename}: {e}")
            return None


# Erstellen Sie eine Instanz des DatabaseManagers, die im Rest Ihres Pakets verwendet wird
db_manager = DatabaseManager()
