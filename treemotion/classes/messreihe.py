from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship
from pathlib import Path

from utilities.base import Base
from .messung import Messung


class Messreihe(Base):
    __tablename__ = 'Messreihe'
    id_messreihe = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    beschreibung = Column(String)
    datum_beginn = Column(DateTime)
    datum_ende = Column(DateTime)
    ort = Column(String)
    anmerkung = Column(String)
    filepaths_tms = Column(String)

    messungen = relationship("Messung", backref="messreihe")

    def __init__(self, id_messreihe=None, beschreibung=None, datum_beginn=None, datum_ende=None, ort=None,
                 anmerkung=None, filepaths_tms=None):
        # in SQLite Database
        self.id_messreihe = id_messreihe
        self.beschreibung = beschreibung
        self.datum_beginn = datum_beginn
        self.datum_ende = datum_ende
        self.ort = ort
        self.anmerkung = anmerkung
        self.filepaths_tms = filepaths_tms
        # additional only in class-object
        self.messungen_list = []

    @classmethod
    def from_database(cls, db_messreihe):
        obj = cls()
        # in SQLite Database
        obj.id_messreihe = db_messreihe.id_messreihe
        obj.beschreibung = db_messreihe.beschreibung
        obj.datum_beginn = db_messreihe.datum_beginn
        obj.datum_ende = db_messreihe.datum_ende
        obj.ort = db_messreihe.ort
        obj.anmerkung = db_messreihe.anmerkung
        obj.filepaths_tms = db_messreihe.filepaths_tms
        # additional only in class-object
        obj.messungen_list = []
        return obj

    def add_messungen(self, session):
        # Alle Messungen aus der Datenbank laden, die der aktuellen Messreihe zugeordnet sind
        db_messungen = session.query(Messung).filter_by(id_messreihe=self.id_messreihe).all()

        # Für jede Messung in der Datenbank eine neue Messung-Instanz erstellen
        # und zur Liste der Messungen in der Messreihe hinzufügen, falls sie noch nicht vorhanden ist
        for db_messung in db_messungen:
            # Überprüfen, ob die Messung bereits in der Liste ist
            if any(messung.id_messung == db_messung.id_messung for messung in self.messungen_list):
                continue

            messung = Messung.from_database(db_messung)
            self.messungen_list.append(messung)

        return self.messungen_list

    def add_filenames(self, session, csv_path, feedback=False):
        if not csv_path:
            if feedback:
                print("Fehler: csv_path ist ungültig.")
            return

        if not self.filepaths_tms:
            if feedback:
                print("Fehler: self.filepaths_tms ist ungültig.")
            return

        csv_path = Path(csv_path)
        filepaths_tms = Path(self.filepaths_tms)
        search_path = csv_path.joinpath(filepaths_tms)
        if feedback:
            print(f"Suche TMS-files in Pfad: {search_path}")

        # Suche nach CSV-Dateien im Verzeichnis und allen Unterordnern
        files = list(search_path.glob('**/*.csv'))
        if feedback:
            print(f"Folgende Files gefunden: {files}")

        # Erstelle eine Liste der id_sensor aus den Dateinamen der gefundenen Dateien
        id_sensor_list = [int(filename.stem[-3:]) for filename in files]
        if feedback:
            print(f"Entspricht folgenden Sensor_ID`s: {id_sensor_list}")

        # Aktualisiere die Datenbank für alle Messungen in der messungen_list
        for messung in self.messungen_list:
            if messung.id_messreihe == self.id_messreihe:  # Prüfe, ob die Messung zur aktuellen Messreihe gehört
                try:
                    sensor_index = id_sensor_list.index(messung.id_sensor)
                except ValueError:
                    if feedback:
                        print(f"Messung {messung.id_messung}: Fehler - Filename nicht gefunden.")
                    continue

                messung.filename = files[sensor_index].name
                messung.filepath = files[sensor_index].resolve()

                session.query(Messung).filter_by(id_messung=messung.id_messung).update(
                    {
                        "filename": messung.filename
                    }
                )
                session.commit()
                if feedback:
                    print(
                        f"Messung {messung.id_messung} (Sensor {messung.id_sensor}): Filename erfolgreich aktualisiert.")
