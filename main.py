# main.py
import logging


from treemotion import Projekt, Messreihe
from utilities.logging_config import configure_logger

# Konfiguriere den Logger zuerst
configure_logger()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    print("Start")
    projekt_data_path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"
    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

    projekt_1 = Projekt.load(id_projekt=1, name="TREEMOTION", path=projekt_data_path)
    projekt_1.add_messreihen()
    #
    # projekt_1.add_filenames(csv_path)
    messreihe_1 = projekt_1.messreihen_list[2]
    # messreihe_1.add_data_from_csv()
    # messreihe_1.add_data_from_db(version="raw")
    messreihe_1.delete_data_version_from_db(version="raw")

    print("Ende")
