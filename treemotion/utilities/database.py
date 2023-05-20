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
    """
    Eine Klasse zur Verwaltung von Datenbankverbindungen und -sitzungen.
    """

    def __init__(self):
        """
        Konstruktor der DatabaseManager-Klasse.
        Initialisiert das engine- und session-Attribut auf None.
        """
        self.engine = None
        self.session_factory = None
        self.current_session = None

    def connect(self, db_path: str):
        """
        Verbindet zur Datenbank und erstellt eine neue session_factory.
        Args:
            db_path (str): Pfad zur Datenbank.
        """
        db_path = Path(db_path)
        DATABASE_URI = f'sqlite:///{db_path}'

        # Überprüfung, ob die Datenbank bereits existiert
        if db_path.is_file():
            logger.info(f"Die existierende Datenbank unter {DATABASE_URI} wird genutzt.")
        else:
            logger.info(f"Die Datenbank unter {DATABASE_URI} existiert nicht und wird erstellt.")

        try:
            self.engine = create_engine(DATABASE_URI)
            Base.metadata.create_all(self.engine)  # Erstellt alle Tabellen, definiert in Ihrem Base ORM-Objekt
            self.session_factory = sessionmaker(bind=self.engine)
            self.current_session = self.session_factory()
            logger.info(f"Verbindung zur Datenbank unter {DATABASE_URI} erfolgreich hergestellt.")
        except Exception as e:
            logger.error(f"Fehler beim Verbinden zur Datenbank: {e}")
            raise e

    def disconnect(self):
        """
        Trennt die Verbindung zur Datenbank.
        """
        if self.engine is not None:
            if self.current_session is not None:
                if len(self.current_session.dirty) > 0 or len(self.current_session.new) > 0:
                    logger.warning(f"Es gibt noch nicht committete Änderungen.")
                    self.ask_commit(self.current_session)
                self.current_session.close()
                self.current_session = None
            self.session_factory = None
            self.engine.dispose()
            self.engine = None
            logger.info("Datenbankverbindung erfolgreich getrennt.")
        else:
            logger.warning("Es gibt keine aktive Verbindung zur Datenbank, die getrennt werden könnte.")

    def get_session(self, session=None):
        """
        Gibt die angegebene Session oder, wenn keine angegeben wurde, die aktuelle Session zurück.
        Args:
            session (Optional[Session]): Eine optionale Session.
        Returns:
            Session: Die angegebene oder die aktuelle Session.
        """
        if self.session_factory is None:
            logger.error("Datenbank ist nicht verbunden. Bitte rufen Sie zuerst connect() auf.")
            raise Exception("Datenbank ist nicht verbunden. Bitte rufen Sie zuerst connect() auf.")

        if session is None:
            # logger.debug("Es wurde keine Session übergeben. Die aktuelle Session wird verwendet.")
            return self.current_session
        else:
            # logger.debug("Es wurde eine Session übergeben. Diese Session wird verwendet.")
            return session

    def commit(self, session=None):
        """
        Committet alle Änderungen in der angegebenen Session zur Datenbank.
        Args:
            session (Session): Die Session, in der die Änderungen vorgenommen wurden.
        """
        session = self.get_session(session)
        try:
            session.commit()
            logger.debug("Änderungen erfolgreich zur Datenbank committet.")
        except Exception as e:
            logger.error("Fehler beim Commit zur Datenbank: ", e)
            raise e

    # Nur für Anwender individuelle Sessions
    def open_session(self):
        """
        Erstellt und gibt eine neue Session zurück.
        Returns:
            Session: Eine neue Session.
        """
        if self.session_factory is None:
            logger.error("Datenbank ist nicht verbunden. Bitte rufen Sie zuerst connect() auf.")
            raise Exception("Datenbank ist nicht verbunden. Bitte rufen Sie zuerst connect() auf.")
        self.current_session = self.session_factory()
        logger.debug("Neue Session erfolgreich erstellt.")
        return self.current_session

    # Nur für Anwender individuelle Sessions
    def close_session(self, session):
        """
        Schließt die angegebene Session.
        Args:
            session (Session): Die zu schließende Session.
        """
        try:
            if len(session.dirty) > 0 or len(session.new) > 0:
                logger.warning(f"Es gibt noch nicht committete Änderungen.")
                self.ask_commit(session)
            session.close()
            if session == self.current_session:
                self.current_session = None
            logger.debug("Session erfolgreich geschlossen.")
        except Exception as e:
            logger.error("Fehler beim Schließen der Session: ", e)
            raise e

    @staticmethod
    def create_template_db(path: str, name: str):
        """
        Erstellt eine neue Datenbank indem die Vorlagendatenbank kopiert wird.
        Args:
            path (str): Der Pfad, in dem die neue Datenbank erstellt werden soll.
            name (str): Der Name der neuen Datenbank.

        Returns:
            str: Der Pfad zur neuen Datenbank oder None, wenn die Datenbank nicht erstellt werden konnte. Bei Fehler = None
        """
        path = Path(path)
        database_filename = f"{name}.db"
        database_path = path / database_filename

        if database_path.exists():
            logger.error(f"Eine Datenbank namens '{database_filename}' existiert bereits in {path}.")
            return str(database_path)

        template_database_filename = configuration.template_db_name
        template_database_path = Path(__file__).parent.parent / template_database_filename

        if not template_database_path.is_file():
            logger.error(f"Die Datei {template_database_filename} wurde nicht gefunden.")
            return

        try:
            copy(template_database_path, database_path)
            logger.info(f"Datenbank '{database_filename}' wurde erfolgreich in {path} erstellt.")
            return str(database_path)
        except Exception as e:
            logger.error(f"Fehler beim Erstellen der Datenbank {database_filename}: ", e)
            return None

    def ask_commit(self, session):
        """
        Fragt den Benutzer, ob er die Änderungen committen möchte.
        Args:
            session (Session): Die Session, in der die Änderungen vorgenommen wurden.
        """
        # START GUI MODIFICATION
        commit = input("Möchten Sie committen? (True/False): ")
        # END GUI MODIFICATION
        if commit.lower() == 'true':
            try:
                session.commit()
                logger.info("Änderungen erfolgreich zur Datenbank committet.")
            except Exception as e:
                logger.error("Fehler beim Commit zur Datenbank: ", e)
                raise e
        else:
            logger.warning("Änderungen wurden nicht zur Datenbank committet und verworfen.")
