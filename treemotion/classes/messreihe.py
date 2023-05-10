# messreihe.py
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

from .messung import Messung
from utilities.base import Base
from utilities.timing import timing_decorator


class Messreihe(Base):
    __tablename__ = 'Messreihe'
    id_messreihe = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    beschreibung = Column(String)
    datum_beginn = Column(DateTime)
    datum_ende = Column(DateTime)
    ort = Column(String)
    anmerkung = Column(String)
    filepath_tms = Column(String)

    messungen = relationship("Messung", backref="messreihe")

    def __init__(self, session, id_messreihe=None, beschreibung=None, datum_beginn=None, datum_ende=None, ort=None,
                 anmerkung=None, filepath_tms=None):
        # Speichern des Sessions-Objektes (Standard aus Projekt-Klasse)
        self.session = session

        # in SQLite Database
        self.id_messreihe = id_messreihe
        self.beschreibung = beschreibung
        self.datum_beginn = datum_beginn
        self.datum_ende = datum_ende
        self.ort = ort
        self.anmerkung = anmerkung
        self.filepath_tms = filepath_tms
        # additional only in class-object
        self.messungen_list = []

    @classmethod
    @timing_decorator
    def from_database(cls, db_messreihe, session):
        obj = cls(session)
        # in SQLite Database
        obj.id_messreihe = db_messreihe.id_messreihe
        obj.beschreibung = db_messreihe.beschreibung
        obj.datum_beginn = db_messreihe.datum_beginn
        obj.datum_ende = db_messreihe.datum_ende
        obj.ort = db_messreihe.ort
        obj.anmerkung = db_messreihe.anmerkung
        obj.filepath_tms = db_messreihe.filepath_tms
        return obj

    @timing_decorator
    def add_messungen(self):
        # Alle Messungen aus der Datenbank laden, die der aktuellen Messreihe zugeordnet sind
        db_messungen = self.session.query(Messung).filter_by(id_messreihe=self.id_messreihe).all()

        # Für jede Messung in der Datenbank eine neue Messung-Instanz erstellen
        # und zur Liste der Messungen in der Messreihe hinzufügen, falls sie noch nicht vorhanden ist
        for db_messung in db_messungen:
            # Überprüfen, ob die Messung bereits in der Liste ist
            if any(messung.id_messung == db_messung.id_messung for messung in self.messungen_list):
                continue

            messung = Messung.from_database(db_messung, self.session)
            self.messungen_list.append(messung)

    @timing_decorator
    def add_filenames(self, csv_path):
        if not csv_path:
            logger.error("Fehler: csv_path ist ungültig.")
            return

        if not self.filepath_tms:
            logger.error("Fehler: self.filepath_tms ist ungültig.")
            return

        csv_path = Path(csv_path)
        filepath_tms = Path(self.filepath_tms)
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

    def check_messungen_list(self):
        if not self.messungen_list:
            logger.warning("Fehler: Es gibt keine Messungen in der Messreihe.")
            return False
        return True

    @timing_decorator
    def add_data_from_csv(self, version="raw"):
        if not self.check_messungen_list():
            return

        for messung in self.messungen_list:
            if messung.filepath:
                try:
                    messung.add_data_from_csv(version=version)
                    logger.info(f"CSV-Daten für Messung {messung.id_messung} wurden erfolgreich hinzugefügt.")
                except Exception as e:
                    logger.warning(f"Fehler beim Hinzufügen von Daten aus CSV für Messung {messung.id_messung}: {e}")
            else:
                logger.warning(f"Fehler: Kein Dateipfad für Messung {messung.id_messung} vorhanden.")

    @timing_decorator
    def add_data_from_db(self, version: str):
        if not self.check_messungen_list():
            return

        for messung in self.messungen_list:
            try:
                messung.add_data_from_db(version=version)
                logger.info(f"DB-Daten für Messung {messung.id_messung} wurden erfolgreich hinzugefügt.")
            except Exception as e:
                logger.warning(
                    f"Fehler beim Hinzufügen von Daten aus der Datenbank für Messung {messung.id_messung}: {e}")

    @timing_decorator
    def delete_data_version_from_db(self, version):

        if not self.check_messungen_list():
            return

        for messung in self.messungen_list:
            try:
                messung.delete_data_version(version)
                logger.info(f"Daten-Version {version} für Messung {messung.id_messung} wurde erfolgreich gelöscht.")
            except Exception as e:
                logger.error(f"Fehler beim Löschen der Daten-Version {version} für Messung {messung.id_messung}: {e}")
