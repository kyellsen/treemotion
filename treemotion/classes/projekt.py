# treemotion/classes/projekt.py


from utilities.common_imports import *

from .messreihe import Messreihe

class Projekt(BaseClass):
    __tablename__ = 'Projekt'
    id_projekt = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)

    messreihen = relationship("Messreihe", backref="projekt", lazy="joined", cascade="all, delete, delete-orphan", order_by="Messreihe.id_messreihe")

    def __init__(self, *args, id_projekt=None, name=None, **kwargs):
        super().__init__(*args, **kwargs)

        # in SQLite Database
        self.id_projekt = id_projekt
        self.name = name

    @classmethod
    @timing_decorator
    def load_from_db(cls, path_db=None, id_projekt=None):
        objs = super().load_from_db(path_db, filter_by={'id_projekt': id_projekt} if id_projekt else None)
        logger.info(f"{len(objs)} Projekte wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def remove_from_db(self, *args, path_db=None):
        # Call the base class method to remove this Data object from the database
        super().remove_from_db(path_db, id_name='id_projekt')

    def copy(self, copy_relationships=True):
        copy = super().copy(copy_relationships=copy_relationships)
        return copy
    @timing_decorator
    def add_filenames(self, csv_path):
        self.for_all('messreihen', 'add_filenames', csv_path)

    @timing_decorator
    def load_data_from_csv(self, version=configuration.data_version_default, overwrite=False):
        self.for_all('messreihen', 'load_data_from_csv', version, overwrite)
