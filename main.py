from treemotion import Projekt, Messreihe, Messung

# import tempdrift
# import treemotion as tms
# from treemotion import Messung

# if __name__ == "__main__":
#     print("Start")
#     test_dir = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\data_tests"
#     csv_file = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2023_Kronensicherung_Plesse\020_Daten\TMS\CSV_Messung_001_Plesse_export_2023-03-22_24h\2023-03-22 000000__DatasA000-0000-0007.csv"
#     db_file = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\data_tests\test.db"
#
#     start_time = pd.Timestamp('2023-03-22 08:00:00')
#     end_time = pd.Timestamp('2023-03-22 10:00:00')
#
#     messung_1 = Messung.read_from_csv(source_path=csv_file, messung_id=1, feedback=True)
#     messung_1.limit_time(start_time, end_time)
#     messung_1.temp_drift_comp(feedback=True)
#     cols = 'East-West-Inclination', 'East-West-Inclination - drift compensated'
#     messung_1.plot_data_sub(y_cols=cols)
#     messung_1.plot_data(y_cols=cols)
#
#     print("Ende")


if __name__ == "__main__":
    print("Start")
    projekt_data_path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"
    projekt_name = "TREEMOTION_03"
    csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

    projekt_1 = Projekt.load(id_projekt=1, name=projekt_name, path=projekt_data_path)
    projekt_1.add_all_messreihen(add_filenames=True, csv_path=csv_path)
    projekt_data_path = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data"


    print("Ende")
