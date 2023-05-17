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
    test_list = projekt.load_data_from_csv(auto_commit=True, overwrite=False)
    #db_manager.disconnect()

    #projekt = projekt_list[0]
    # Hier würden Änderungen an Projekt-Instanz erfolgen. Projekt erbt von BaseClass.
    #projekt.commit_to_db()
    # projekt.add_filenames(csv_path)
    #projekt.load_data_from_csv(overwrite=True)

    print("Ende")
