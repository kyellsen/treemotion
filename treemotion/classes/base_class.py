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
        results = []  # Store the method return values here
        for obj in getattr(self, list_name):
            method = getattr(obj, method_name, None)
            if callable(method):
                result = method(*args, **kwargs)
                results.append(result)  # Append the method return value to the list
            else:
                logger.error(f"Die Methode {method_name} existiert nicht in der Klasse {obj.__class__.__name__}.")
                return None
        return results  # Return the list of method return values

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

    def copy(self, id_name='id', reset_id=False, auto_commit=False, session=None):
        new_obj = self.__class__()

        for attr, value in self.__dict__.items():
            if attr == '_sa_instance_state':
                continue

            setattr(new_obj, attr, value)

        if reset_id:
            try:
                setattr(new_obj, id_name, None)
                logger.debug(f"Reset id attribute {id_name} in new instance.")
            except Exception as e:
                logger.error(f"Error resetting id attribute {id_name}: {e}")

        if auto_commit:
            setattr(new_obj, id_name, None)
            session = db_manager.get_session(session)
            try:
                session.add(new_obj)
                db_manager.commit(session)
                logger.info(f"New instance of {self.__class__.__name__} added to session and committed.")
            except Exception as e:
                session.rollback()
                logger.error(f"Error committing new instance of {self.__class__.__name__}: {e}")

        return new_obj

    def copy_deep(self, copy_relationships=True):
        new_obj = self.__class__()

        for attr, value in self.__dict__.items():
            if attr == '_sa_instance_state':
                continue

            try:
                if isinstance(value, list) and copy_relationships:
                    setattr(new_obj, attr, [item.copy() for item in value])
                    logger.debug(f"Copied list attribute {attr} from {self.__class__.__name__} to new instance.")
                elif isinstance(value, BaseClass) and copy_relationships:
                    setattr(new_obj, attr, value.copy())
                    logger.debug(f"Copied BaseClass attribute {attr} from {self.__class__.__name__} to new instance.")
                else:
                    setattr(new_obj, attr, value)
                    logger.debug(f"Copied attribute {attr} from {self.__class__.__name__} to new instance.")
            except Exception as e:
                logger.error(f"Error copying attribute {attr}: {e}")
                continue

        return new_obj

    def remove(self, id_name='id', auto_commit=False, session=None):
        session = db_manager.get_session(session)
        existing_obj = session.query(type(self)).get(getattr(self, id_name))
        try:
            if existing_obj is not None:
                session.delete(existing_obj)
                logger.info(f"Objekt {self.__class__.__name__} wurde entfernt.")
            else:
                logger.info(f"Objekt {self.__class__.__name__} ist nicht vorhanden.")
            if auto_commit:
                db_manager.commit(session)
        except Exception as e:
            session.rollback()  # Rollback the changes on error
            logger.error(f"Fehler beim Entfernen des Objekts {self.__class__.__name__}: {e}")
