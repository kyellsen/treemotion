# main.py
import treemotion
from treemotion import db_manager, Projekt, Messreihe, Messung, Data

if __name__ == "__main__":
    print("Start")
    path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION.db"
    path_123 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\test_123.db"
    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"


    db_manager.connect(path_db_1)
    projekt_list = Projekt.load_from_db()
    projekt = projekt_list[0]
    messreihe = projekt.messreihen[0]
    messung = messreihe.messungen[0]
    result = messung.load_data_version(version="raw")
    copy = messung.copy_version(version_new="test_version", version_source="raw")
    # results_list = messreihe.load_data_version(version="raw")
    # copy_list = messreihe.copy_version(version_new="test_version", version_source="raw")
    # db_manager.disconnect()

    print("Ende")
