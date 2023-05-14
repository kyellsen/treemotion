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
    def load_from_db(cls, path_db, filter_by=None, load_related=None, related_attribute=None):
        session = create_session(path_db)

        if filter_by is not None:
            if load_related:
                objs = session.query(cls).options(joinedload(related_attribute)).filter_by(**filter_by).all()
            else:
                objs = session.query(cls).filter_by(**filter_by).all()
        else:
            if load_related:
                objs = session.query(cls).options(joinedload(related_attribute)).all()
            else:
                objs = session.query(cls).all()

        session.close()
        logger.info(f"{len(objs)} {cls.__name__} Objekte wurden erfolgreich geladen.")
        return objs

    # BaseClass
    def add_to_db(self, path_db, id_name='id', update=False):
        session = create_session(path_db)
        existing_obj = session.query(type(self)).get(getattr(self, id_name))

        if existing_obj is None:
            session.add(self)
            logger.info(f"Objekt {self.__class__.__name__} wurde der Datenbank hinzugefügt: {path_db}")
        elif update and self != existing_obj:
            session.merge(self)
            logger.info(f"Objekt {self.__class__.__name__} wurde in der Datenbank aktualisiert: {path_db}")
        else:
            logger.info(
                f"Objekt {self.__class__.__name__} ist bereits in der Datenbank vorhanden und wurde nicht geändert: {path_db}")

        session.commit()
        session.close()

    def remove_from_db(self, path_db, id_name='id'):
        session = create_session(path_db)
        existing_obj = session.query(type(self)).get(getattr(self, id_name))

        if existing_obj is not None:
            session.delete(existing_obj)
            logger.info(f"Objekt {self.__class__.__name__} wurde aus der Datenbank entfernt.")
            session.commit()
        else:
            logger.info(f"Objekt {self.__class__.__name__} ist nicht in der Datenbank vorhanden.")

        session.close()
