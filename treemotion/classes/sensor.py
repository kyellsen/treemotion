class Sensor:
    def __init__(self):
        self.id = int()
        self.ort = str()
        self.hoehe = int()
        self.umfang = int()
        self.ausrichtung = None
        self.messungen = []  # Liste von Messung-Objekten

    @classmethod
    def from_database(cls, db_connection, sensor_id):
        # Hier sollte der Code zum Auslesen der Daten aus der SQLite-Datenbank mittels Pandas und SQLAlchemy eingefügt werden
        pass

if __name__ == "__main__":
    pass
