from pathlib import Path
import shutil
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .messreihe import Messreihe


class Projekt:
    def __init__(self):
        # only in class-object
        self.id = int()
        self.name = str()
        self.path = Path()
        self.engine = None
        self.session = None
        self.messreihen_list = []

    @classmethod
    def create(cls, id_projekt: int, name: str, path: str):
        obj = cls()
        obj.id = id_projekt
        obj.name = name
        obj.path = Path(path)

        # Verzeichnis erstellen, falls nicht vorhanden
        obj.path.mkdir(parents=True, exist_ok=True)

        obj.path_db = obj.path / f"{name}.db"

        # Ermitteln des Pfads zur template.db-Datei
        current_file = Path(__file__)
        parent_directory = current_file.parent.parent
        template_db_path = parent_directory / 'template.db'

        if not template_db_path.exists() or not template_db_path.is_file():
            print("Fehler: Die template.db-Datei wurde nicht gefunden.")
            return None

        # Kopieren der template.db-Datei, um eine neue Datenbank zu erstellen
        try:
            shutil.copy(template_db_path, obj.path_db)
        except Exception as e:
            print(f"Fehler beim Kopieren der template.db-Datei: {e}")
            return None

        # Erstellen einer Verbindung zur neuen Datenbank
        try:
            obj.engine = create_engine(f"sqlite:///{obj.path_db}")
            Session = sessionmaker(bind=obj.engine)
            obj.session = Session()
        except Exception as e:
            print(f"Fehler beim Herstellen der Verbindung zur Datenbank: {e}")
            return None

        return obj

    @classmethod
    def load(cls, id_projekt: int, name: str, path: str):
        obj = cls()
        obj.id = id_projekt
        obj.name = name
        obj.path = Path(path)
        obj.path_db = obj.path / f"{name}.db"

        # Überprüfen, ob die angegebene Datenbank existiert
        if not obj.path_db.exists() or not obj.path_db.is_file():
            print(f"Fehler: Die angegebene Datenbank {obj.path_db} wurde nicht gefunden.")
            return None

        # Erstellen einer Verbindung zur angegebenen Datenbank
        try:
            obj.engine = create_engine(f"sqlite:///{obj.path_db}")
            Session = sessionmaker(bind=obj.engine)
            obj.session = Session()
        except Exception as e:
            print(f"Fehler beim Herstellen der Verbindung zur Datenbank: {e}")
            return None

        return obj

    def add_messreihe(self, id_messreihe):
        # Überprüfen, ob die Messreihe bereits in der Liste ist
        if any(messreihe.id_messreihe == id_messreihe for messreihe in self.messreihen_list):
            print(f"Messreihe mit ID {id_messreihe} bereits hinzugefügt")
            return

        # Daten der Messreihe aus der Datenbank laden
        db_messreihe = self.session.query(Messreihe).filter_by(id_messreihe=id_messreihe).first()
        if db_messreihe is None:
            print(f"Messreihe mit ID {id_messreihe} nicht gefunden")
            return

        # Neue Instanz von Messreihe erstellen und zur Liste hinzufügen
        messreihe = Messreihe.from_database(db_messreihe)
        messreihe.add_all_messungen(self.session)  # Füge alle Messungen zur Messreihe hinzu
        self.messreihen_list.append(messreihe)

    def add_all_filenames(self, csv_path):
        for messreihe in self.messreihen_list:
            try:
                print(f"Messreihe {messreihe.id_messreihe}: füge Filenames hinzu")
                messreihe.add_filenames(self.session, csv_path)
                print("\n")
            except Exception as e:
                print(f"Fehler beim Hinzufügen der Filenames zu Messreihe {messreihe.id_messreihe}: {e}")

    def add_all_messreihen(self, add_filenames=False, csv_path=None):
        # Alle Messreihen aus der Datenbank laden
        db_messreihen = self.session.query(Messreihe).all()

        # Für jede Messreihe in der Datenbank eine neue Messreihe-Instanz erstellen
        # und zur Liste der Messreihen im Projekt hinzufügen, falls sie noch nicht vorhanden ist
        for db_messreihe in db_messreihen:
            # Überprüfen, ob die Messreihe bereits in der Liste ist
            if any(messreihe.id_messreihe == db_messreihe.id_messreihe for messreihe in self.messreihen_list):
                print(
                    f"Messreihe mit ID {db_messreihe.id_messreihe} bereits hinzugefügt")  # Fehlermeldung, wenn Messreihe bereits hinzugefügt
                continue

            messreihe = Messreihe.from_database(db_messreihe)
            messreihe.add_all_messungen(self.session)  # Füge alle Messungen zur Messreihe hinzu
            self.messreihen_list.append(messreihe)

            if add_filenames:
                if csv_path is None:
                    print("Fehler: csv_path ist erforderlich, wenn add_filenames=True.")
                    return
                self.add_all_filenames(csv_path)

