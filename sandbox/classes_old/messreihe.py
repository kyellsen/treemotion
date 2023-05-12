from pathlib import Path
import pandas as pd
from sqlalchemy import types

import dbi

class Messreihe:
    def __init__(self, id_messreihe: int):
        # from SQLite table
        self.id_messreihe = id_messreihe
        self.beschreibung = str()
        self.datum_beginn = None
        self.datum_ende = None
        self.ort = str()
        self.anmerkung = str()
        self.filepath_tms = Path()
        # treemotion intern referenz
        self.baum_list = list()

    @classmethod
    def from_database(cls, db_connection, messreihe_id):
        # Hier sollte der Code zum Auslesen der Daten aus der SQLite-Datenbank mittels Pandas und SQLAlchemy eingefügt werden
        pass



if __name__ == "__main__":
    pass
