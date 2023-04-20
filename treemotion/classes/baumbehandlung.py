class Baumbehandlung:
    def __init__(self):
        self.id = int()
        self.datum_aufbau = None
        self.datum_abbau = None
        self.sensors = list()

    @classmethod
    def from_database(cls, db_connection, sensor_id):
        # Hier sollte der Code zum Auslesen der Daten aus der SQLite-Datenbank mittels Pandas und SQLAlchemy eingefügt werden
        pass

if __name__ == "__main__":
    pass
