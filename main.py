# main.py
from treemotion import Projekt
from utilities.timing import timing_decorator

from utilities.log import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    print("Start")
    projekt_data_path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"
    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

    projekt_2 = Projekt.create(1, "Test", projekt_data_path)
    # projekt_1 = Projekt.load_complete(id_projekt=1, name="TREEMOTION", path=projekt_data_path, csv_path=csv_path)

    # messreihe_1 = projekt_1.messreihen_list[2]
    # messreihe_1.add_data_from_csv(csv_path, version="raw")
    # # messreihe_1.delete_data_version_from_db(version="raw")
    # messreihe_1 = projekt_1.messreihen_list[2]
    # messreihe_1.add_data_from_csv(csv_path, version="raw")

    # projekt_1.load_complete(id_projekt=1, name="TREEMOTION", path=projekt_data_path, csv_path=csv_path)
    # projekt_1 = Projekt.from_database(id_projekt=1, name="TREEMOTION", path=projekt_data_path)
    # projekt_1.add_messreihen()
    #
    # projekt_1.add_filenames(csv_path)
    # messreihe_1 = projekt_1.messreihen_list[2]
    # messreihe_1.add_data_from_csv(csv_path)
    # messreihe_1.add_data_from_db(version="raw")

    print("Ende")
