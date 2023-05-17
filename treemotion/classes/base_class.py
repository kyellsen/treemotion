# treemotionen/classes/base_class.py
from utilities.imports_classes import *
from utilities.base import Base

logger = get_logger(__name__)

class BaseClass(Base):
    __abstract__ = True  # mark class as abstract

    def __init__(self, *args, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)

    def for_all(self, list_name, method_name, *args, **kwargs):
        for obj in getattr(self, list_name):
            method = getattr(obj, method_name, None)
            if callable(method):
                method(*args, **kwargs)
            else:
                logger.error(f"Die Methode {method_name} existiert nicht in der Klasse {obj.__class__.__name__}.")
                return

    @classmethod
    @timing_decorator
    def load_from_db(cls, filter_by=None, session=None):
        session = db_manager.get_session(session)
        if filter_by is not None:
            objs = session.query(cls).filter_by(**filter_by).all()
        else:
            objs = session.query(cls).all()

        logger.info(f"{len(objs)} {cls.__name__} Objekte wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def commit_to_db(self, refresh=True, session=None):
        session = db_manager.get_session(session)
        session.merge(self)
        if refresh:
            session.refresh(self)
        logger.info(
            f"Änderungen am {self.__class__.__name__} wurden erfolgreich in der Datenbank committet.")

    def remove_from_db(self, id_name='id', session=None):
        session = db_manager.get_session(session)
        existing_obj = session.query(type(self)).get(getattr(self, id_name))
        try:
            if existing_obj is not None:
                session.delete(existing_obj)
                logger.info(f"Objekt {self.__class__.__name__} wurde aus der Datenbank entfernt.")
            else:
                logger.info(f"Objekt {self.__class__.__name__} ist nicht in der Datenbank vorhanden.")
        except Exception as e:
            logger.error(f"Fehler beim Entfernen des Objekts {self.__class__.__name__} aus der Datenbank: {e}")


    def copy(self, copy_relationships=True):
        new_instance = self.__class__()

        for attr, value in self.__dict__.items():
            if attr == '_sa_instance_state':
                continue

            if isinstance(value, list) and copy_relationships:
                setattr(new_instance, attr, [item.copy() for item in value])
            elif isinstance(value, BaseClass) and copy_relationships:
                setattr(new_instance, attr, value.copy())
            else:
                setattr(new_instance, attr, value)

        return new_instance
