class Baum:
    def __init__(self):
        self.id = int()
        self.datum_erhebung = None
        self.bhd = int()
        self.hoehe = int()
        self.zwiesel_hoehe = int()
        self.baumbehandlung = list()

    @classmethod
    def from_database(cls, db_connection, baum_id):
        # Hier sollte der Code zum Auslesen der Daten aus der SQLite-Datenbank mittels Pandas und SQLAlchemy eingefügt werden
        pass

if __name__ == "__main__":
    pass
