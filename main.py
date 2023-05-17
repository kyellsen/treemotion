# main.py
from treemotion import *

if __name__ == "__main__":
    print("Start")
    path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION_1.db"
    path_123 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\test_123.db"

    db_manager.connect_db("/path/to/database.db")
    models.load_data()
    models.edit_data()
    db_manager.commit_to_db()
    db_manager.disconnect_db()

    #projekt = projekt_list[0]
    # Hier würden Änderungen an Projekt-Instanz erfolgen. Projekt erbt von BaseClass.
    #projekt.commit_to_db()
    # projekt.add_filenames(csv_path)
    #projekt.load_data_from_csv(overwrite=True)

    print("Ende")
