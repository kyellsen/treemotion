# treemotion/classes/projekt.py


from utilities.common_imports import *
from .messreihe import Messreihe


class Projekt(BaseClass):
    __tablename__ = 'Projekt'
    id_projekt = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)

    messreihen_list = relationship("Messreihe", backref="projekt", lazy='select', cascade="all, delete, delete-orphan")

    def __init__(self, *args, path_db, id_projekt=None, name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.path_db = Path(path_db)

        # in SQLite Database
        self.id_projekt = id_projekt
        self.name = name

    @classmethod
    @timing_decorator
    def load_from_db(cls, path_db, id_projekt=None, load_related=configuration.projekt_load_related_default):
        objs = super().load_from_db(path_db, filter_by={'id_projekt': id_projekt} if id_projekt else None,
                                    load_related=load_related, related_attribute=cls.messreihen_list)
        for obj in objs:
            obj.path_db = Path(path_db)
        logger.info(f"{len(objs)} Projekte wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def add_to_db(self, *args, path_db, update=False):
        super().add_to_db(path_db, id_name='id_projekt', update=update)

    @timing_decorator
    def remove_from_db(self, *args, path_db):
        # Call the base class method to remove this Data object from the database
        super().remove_from_db(path_db, id_name='id_projekt')
