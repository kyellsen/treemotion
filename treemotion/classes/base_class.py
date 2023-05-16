# treemotionen/classes/base_class.py
from sqlalchemy.ext.declarative import declarative_base
from utilities.common_imports import *

Base = declarative_base()


class BaseClass(Base):
    __abstract__ = True  # mark class as abstract

    def __init__(self, *args, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)

    @classmethod
    @timing_decorator
    def load_from_db(cls, path_db=None, filter_by=None):
        path_db = get_default_path_db(path_db)
        session = create_session(path_db)

        if filter_by is not None:
            objs = session.query(cls).filter_by(**filter_by).all()
        else:
            objs = session.query(cls).all()

        session.close()
        logger.info(f"{len(objs)} {cls.__name__} Objekte wurden erfolgreich geladen.")
        return objs

    def for_all(self, list_name, method_name, *args, **kwargs):
        for obj in getattr(self, list_name):
            method = getattr(obj, method_name, None)
            if callable(method):
                method(*args, **kwargs)
            else:
                logger.error(f"Die Methode {method_name} existiert nicht in der Klasse {obj.__class__.__name__}.")
                return

    @timing_decorator
    def commit_to_db(self, path_db=None, refresh=True):
        path_db = get_default_path_db(path_db)

        try:
            with create_session(path_db) as session:
                session.merge(self)
                session.commit()
                if refresh:
                    session.refresh(self)
                logger.info(
                    f"Änderungen am {self.__class__.__name__} wurden erfolgreich in der Datenbank committet: {path_db}")
        except SQLAlchemyError as e:
            logger.error(f"Fehler beim Committen der Änderungen am {self.__class__.__name__} in der Datenbank: {e}")

    def remove_from_db(self, path_db=None, id_name='id'):
        path_db = get_default_path_db(path_db)

        try:
            with create_session(path_db) as session:
                existing_obj = session.query(type(self)).get(getattr(self, id_name))

                if existing_obj is not None:
                    session.delete(existing_obj)
                    session.commit()
                    logger.info(f"Objekt {self.__class__.__name__} wurde aus der Datenbank entfernt.")
                else:
                    logger.info(f"Objekt {self.__class__.__name__} ist nicht in der Datenbank vorhanden.")
        except SQLAlchemyError as e:
            logger.error(f"Fehler beim Entfernen des Objekts {self.__class__.__name__} aus der Datenbank: {e}")

    def copy(self, copy_relationships=True):
        copy = self.__class__()
        attrs = self.__dict__.copy()
        attrs.pop('_sa_instance_state', None)
        for attr, value in attrs.items():
            if isinstance(value, list) and copy_relationships and value:
                setattr(copy, attr, [item.copy(copy_relationships=True) for item in value])
            elif isinstance(value, BaseClass) and copy_relationships:
                setattr(copy, attr, value.copy(copy_relationships=True))
            else:
                setattr(copy, attr, value)
        return copy


