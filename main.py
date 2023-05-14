# main.py
import treemotion
import copy

if __name__ == "__main__":
    print("Start")
    path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"
    path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION.db"
    path_db_2 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION_spiel.db"
    path_db_3 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\test_1.db"

    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

    projekt_list = treemotion.Projekt.load_from_db(path_db=path_db_2)
    projekt_1 = projekt_list[0]
    # projekt_2 = projekt_list[1]
    messreihe_1 = projekt_1.messreihen_list[1]

    # messreihe_1.add_filenames(csv_path=csv_path)






    print("Ende")

    # projekt_1 = Projekt.load_complete(id_projekt=1, name="TREEMOTION", path=projekt_data_path, csv_path=csv_path)

    # messreihe_1 = projekt_1.messreihen_list[2]
    # messreihe_1.add_data_from_csv(csv_path, version="raw")
    # # messreihe_1.delete_data_version_from_db(version="raw")
    # messreihe_1 = projekt_1.messreihen_list[2]
    # messreihe_1.add_data_from_csv(csv_path, version="raw")

    # projekt_1.load_complete(id_projekt=1, name="TREEMOTION", path=projekt_data_path, csv_path=csv_path)
    # projekt_1 = Projekt.load_from_db(id_projekt=1, name="TREEMOTION", path=projekt_data_path)
    # projekt_1.add_messreihen()
    #
    # projekt_1.add_filenames(csv_path)
    # messreihe_1 = projekt_1.messreihen_list[2]
    # messreihe_1.add_data_from_csv(csv_path)
    # messreihe_1.add_data_from_db(version="raw")
