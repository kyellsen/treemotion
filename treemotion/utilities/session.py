# treemotion/utilities/session.py
"""
session.py

Dieses Modul enthält Funktionen zur Erstellung einer Datenbankverbindungssitzung mittels SQLAlchemy.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .log import get_logger

logger = get_logger(__name__)


def create_session(path_db):
    """
    Erstellt eine Datenbankverbindungssitzung.

    Args:
        path_db (str): Der Pfad zur SQLite-Datenbank.

    Returns:
        session (sqlalchemy.orm.session.Session): Die erstellte Datenbankverbindungssitzung.

    Raises:
        Exception: Wenn ein Fehler beim Herstellen der Verbindung zur Datenbank auftritt.

    """
    try:
        engine = create_engine(f"sqlite:///{path_db}")
        session = sessionmaker(bind=engine)
        logger.info(f"Verbindung zur Datenbank erfolgreich hergestellt.")
    except Exception as e:
        logger.error(f"Fehler beim Herstellen der Verbindung zur Datenbank: {e}")
        return None
    return session()
