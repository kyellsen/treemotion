from utilities.common_imports import *

class BaseClass(Base):
    __tablename__ = ''

    def __init__(self, *args, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)

    @classmethod
    @timing_decorator
    def from_database(cls, path_db, filter_by=None, load_related=None, related_attribute=None):
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
