# treemotion/classes/projekt.py

from utilities.imports_classes import *

from .messreihe import Messreihe

logger = get_logger(__name__)


class Projekt(BaseClass):
    __tablename__ = 'Projekt'
    id_projekt = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)

    messreihen = relationship("Messreihe", backref="projekt", lazy="joined", cascade="all, delete, delete-orphan",
                              order_by="Messreihe.id_messreihe")

    def __init__(self, *args, id_projekt=None, name=None, **kwargs):
        super().__init__(*args, **kwargs)

        # in SQLite Database
        self.id_projekt = id_projekt
        self.name = name

    def __str__(self):
        return f"Projekt(id={self.id_projekt}, name={self.name}"

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_projekt=None, session=None):
        session = db_manager.get_session(session)
        objs = super().load_from_db(filter_by={'id_projekt': id_projekt} if id_projekt else None, session=session)
        logger.info(f"{len(objs)} Projekte wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def remove(self, id_projekt='id_projekt', auto_commit=False, session=None):
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        super().remove(id_projekt, auto_commit, session)

    @timing_decorator
    def copy(self, id_name="id_projekt", reset_id=False, auto_commit=False, session=None):
        new_instance = super().copy(id_name, reset_id, auto_commit, session)
        return new_instance


    def copy_deep(self, copy_relationships=True):
        copy = super().copy_deep(copy_relationships=copy_relationships)
        return copy

    @timing_decorator
    def add_filenames(self, csv_path):
        self.for_all('messreihen', 'add_filenames', csv_path=csv_path)

    @timing_decorator
    def load_data_from_csv(self, version=configuration.data_version_default, overwrite=False, session=None):
        self.for_all('messreihen', 'load_data_from_csv', version, overwrite, session=session)
