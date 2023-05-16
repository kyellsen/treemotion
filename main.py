# main.py
from treemotion import *
# import copy

if __name__ == "__main__":
    print("Start")
    path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"
    path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION.db"
    path_db_2 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION_1.db"
    path_db_3 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION_2.db"
    path_db_3 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION_4.db"

    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

    configuration.default_path_db = path_db_2
    projekt_list = Projekt.load_from_db()
    projekt = projekt_list[0]
    projekt.add_filenames(csv_path)
    projekt.load_data_from_csv(overwrite=False)

    print("Ende")
