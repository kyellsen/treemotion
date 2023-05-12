

class SensorPosition:
    def __init__(self, id_sensor: int):
        # from SQLite table
        self.id_sensor_position = id_sensor

        self.ort = str()
        self.hoehe = int()
        self.umfang = int()
        self.ausrichtung = int()

        # treemotion intern referenz
        self.behandlung = None
        self.messung_list = list()

    @classmethod
    def from_database(cls, db_path: str, table_name: str, id: int):
        pass

    @classmethod
    def get_sensor_manuell(cls, id: int, baumbehandlung: object, ort: str, hoehe: int, umfang: int, ausrichtung: str):
        pass
