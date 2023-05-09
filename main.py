from treemotion import Projekt

if __name__ == "__main__":
    print("Start")
    projekt_data_path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"
    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

    projekt_1 = Projekt.load(id_projekt=1, name="TREEMOTION", path=projekt_data_path)
    projekt_1.add_messreihen(feedback=False)
    projekt_1.add_filenames(csv_path, feedback=False)
    messreihe_1 = projekt_1.messreihen_list[2]
    # messung_1 = projekt_1.messreihen_list[0].messungen_list[0]
    # messreihe_1.add_data_from_csv()
    messreihe_1.add_data_from_db(version="raw")
    messreihe_1.add_data_from_db(version="raw")
    messreihe_1.add_data_from_db(version="raw")



    print("Ende")
