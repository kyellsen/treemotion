import dbi
from messung import Messung

class Sensor:
    def __init__(self):
        self.id = int()
        self.ort = str()
        self.hoehe = int()
        self.umfang = int()
        self.ausrichtung = None
        self.messungen = []

    @classmethod
    def from_database(cls, db_path: str, table_name: str, id: int):

        pass

    @classmethod
    def get_sensor_manuell(cls, id: int, ort: str, hoehe: int, umfang: int, ausrichtung: str, messung: object):
        pass


