# main.py
import treemotion as tms

if __name__ == "__main__":
    print("Start")
    path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"
    path_123 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\test_123.db"
    path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION_1.db"

    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"


    tms.db_manager.connect_db(db_path=path_db_1, set_as_default=True)

    projekt_list = tms.Projekt.load_from_db()
    #projekt = projekt_list[0]
    # Hier würden Änderungen an Projekt-Instanz erfolgen. Projekt erbt von BaseClass.
    #projekt.commit_to_db()
    # projekt.add_filenames(csv_path)
    #projekt.load_data_from_csv(overwrite=True)

    print("Ende")
