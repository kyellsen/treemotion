from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def connect_to_database(path_db):
    try:
        engine = create_engine(f"sqlite:///{path_db}")
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception as e:
        error_message = f"Fehler beim Herstellen der Verbindung zur Datenbank: {e}"
        print(error_message)
        return None, error_message
    return session, None
