class Behandlung:
    def __init__(self, id_behandlung: int):
        # from SQLite table
        self.id_behandlung = id_behandlung
        self.datum_aufbau = None
        self.datum_abbau = None
        self.id_baum = int()
        self.id_behandlungsvariante = int()

        self.behandlungsvariante = str()
        # treemotion intern referenz
        self.baum = None
        self.sensor_list = list()


    @classmethod
    def from_database(cls, db_connection, sensor_id):
        # Hier sollte der Code zum Auslesen der Daten aus der SQLite-Datenbank mittels Pandas und SQLAlchemy eingefügt werden
        pass

class BehandlungKronensicherung(Behandlung):
    def __init__(self, id_behandlung: int):
        super().__init__(id_behandlung)

        # from SQLite table
        self.ks_hoehe = int()
        self.ks_laenge = int()
        self.ks_sa_umfang = int()
        self.ks_sb_umfang = int()
        self.ks_durchhang = int()

    # Zusätzliche Methoden hinzufügen
    def zusatzmethode(self):
        pass


if __name__ == "__main__":
    pass
