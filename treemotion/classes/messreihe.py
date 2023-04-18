from pathlib import Path


class Messreihe:
    def __init__(self):
        self.id = int()
        self.beschreibung = str()
        self.datum_beginn = None
        self.datum_ende = None
        self.ort = str()
        self.filepath = Path()
        self.baeume = []  # Liste von Baum-Objekten

    @classmethod
    def from_database(cls, db_connection, messreihe_id):
        # Hier sollte der Code zum Auslesen der Daten aus der SQLite-Datenbank mittels Pandas und SQLAlchemy eingefügt werden
        pass

if __name__ == "__main__":
    pass
