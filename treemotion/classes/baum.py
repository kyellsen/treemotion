class Baum:
    def __init__(self, id_baum: int):
        # from SQLite table
        self.id_baum = id_baum
        self.datum_erhebung = None
        self.art = str()
        self.bhd = int()
        self.hoehe = int()
        self.zwiesel_hoehe = int()
        # treemotion intern referenz
        self.messreihe = None
        self.id_messreihe = int()
        self.behandlung_list = list()

    @classmethod
    def from_database(cls, db_connection, baum_id):
        # Hier sollte der Code zum Auslesen der Daten aus der SQLite-Datenbank mittels Pandas und SQLAlchemy eingefügt werden
        pass

if __name__ == "__main__":
    pass
