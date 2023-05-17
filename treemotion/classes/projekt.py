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

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_projekt=None, session=None):
        session = db_manager.get_session(session)
        objs = super().load_from_db(filter_by={'id_projekt': id_projekt} if id_projekt else None, session=session)
        logger.info(f"{len(objs)} Projekte wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def remove_from_db(self, session=None):
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        super().remove_from_db(id_name='id_projekt', session=session)

    def copy(self, copy_relationships=True):
        copy = super().copy(copy_relationships=copy_relationships)
        return copy

    @timing_decorator
    def add_filenames(self, csv_path, session=None):
        self.for_all('messreihen', 'add_filenames', csv_path, session=session)

    @timing_decorator
    def load_data_from_csv(self, version=configuration.data_version_default, overwrite=False, session=None):
        self.for_all('messreihen', 'load_data_from_csv', version, overwrite, session=session)
