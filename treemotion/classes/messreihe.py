# treemotion/classes/messreihe.py
from pathlib import Path

from utilities.common_imports import *

from .messung import Messung


class Messreihe(BaseClass):
    __tablename__ = 'Messreihe'
    id_messreihe = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_projekt = Column(Integer, ForeignKey('Projekt.id_projekt'))
    beschreibung = Column(String)
    datum_beginn = Column(DateTime)
    datum_ende = Column(DateTime)
    ort = Column(String)
    anmerkung = Column(String)
    filepath_tms = Column(String)

    messungen_list = relationship("Messung", backref="messreihe", lazy='joined', cascade='all, delete, delete-orphan')

    def __init__(self, *args, id_messreihe=None, beschreibung=None, datum_beginn=None, datum_ende=None, ort=None,
                 anmerkung=None, filepath_tms=None, **kwargs):
        super().__init__(*args, **kwargs)

        # in SQLite Database
        self.id_messreihe = id_messreihe
        self.beschreibung = beschreibung
        self.datum_beginn = datum_beginn
        self.datum_ende = datum_ende
        self.ort = ort
        self.anmerkung = anmerkung
        self.filepath_tms = filepath_tms
    @classmethod
    @timing_decorator
    def load_from_db2(cls, path_db, id_projekt=None, load_related=True):
        objs = super().load_from_db(path_db, filter_by=None,
                                    load_related=load_related, related_attribute=Messreihe.messungen_list)
        logger.info(f"{len(objs)} Messreihen wurden erfolgreich geladen.")
        return objs
    @classmethod
    @timing_decorator
    def load_from_db2(cls, path_db, id_projekt=None, load_related=configuration.messreihe_load_related_default):
        objs = super().load_from_db(path_db, filter_by={'id_projekt': id_projekt} if id_projekt else None,
                                    load_related=load_related, related_attribute=cls.messungen_list)
        logger.info(f"{len(objs)} Messreihen wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def add_to_db(self, *args, path_db, update=False):
        super().add_to_db(path_db, id_name='id_messreihe', update=update)

    @timing_decorator
    def remove_from_db(self, *args, path_db):
        # Call the base class method to remove this Data object from the database
        super().remove_from_db(path_db, id_name='id_messreihe')

    @timing_decorator
    def add_filenames(self, csv_path: Path):

        if not isinstance(csv_path, Path):
            try:
                csv_path = Path(csv_path)
                logger.debug(f"Path korrekt: {csv_path} ")
            except Exception as e:
                logger.error(f"Parameter csv_path kann nicht nicht mit Path(csv_path) umgewandelt werden. Fehler: {e}")
                return

        if not isinstance(self.filepath_tms, Path):
            try:
                filepath_tms = Path(self.filepath_tms)
                logger.debug(f"Path korrekt: {self.filepath_tms} ")
            except Exception as e:
                logger.error(f"Parameter csv_path kann nicht nicht mit Path(csv_path) umgewandelt werden. Fehler: {e}")
                return

        search_path = csv_path.joinpath(filepath_tms)
        logger.info(f"Suche TMS-files in Pfad: {search_path}")

        # Suche nach CSV-Dateien im Verzeichnis und allen Unterordnern
        files = list(search_path.glob('**/*.csv'))
        logger.info(f"Folgende Files gefunden: {files}")

        # Erstelle eine Liste der id_sensor aus den Dateinamen der gefundenen Dateien
        id_sensor_list = [int(filename.stem[-3:]) for filename in files]
        logger.info(f"Entspricht folgenden Sensor_ID`s: {id_sensor_list}")

        # Aktualisiere die Datenbank für alle Messungen in der messungen_list
        for messung in self.messungen_list:
            if messung.id_messreihe == self.id_messreihe:  # Prüfe, ob die Messung zur aktuellen Messreihe gehört
                try:
                    sensor_index = id_sensor_list.index(messung.id_sensor)
                    messung.filename = files[sensor_index].name
                    messung.filepath = files[sensor_index].resolve()

                    self.session.query(Messung).filter_by(id_messung=messung.id_messung).update(
                        {
                            "filename": messung.filename,
                            "filepath": str(messung.filepath)
                        }
                    )
                    self.session.commit()
                    logger.info(
                        f"Messung {messung.id_messung} (Sensor {messung.id_sensor}): Filename und Filepath erfolgreich aktualisiert.")
                except ValueError:
                    logger.warning(f"Messung {messung.id_messung}: Fehler - Filename nicht gefunden.")
                    continue
