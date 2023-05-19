# main.py
import treemotion
from treemotion import db_manager, Projekt, Messreihe, Messung, Data

print("Start")

path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION.db"
csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

db_manager.connect(path_db_1)
projekt_list = Projekt.load_from_db()
projekt = projekt_list[0]
projekt.add_filenames(csv_path=csv_path)
# projekt.load_data_from_csv(version="raw", overwrite=False, auto_commit=True)
messreihe_1 = projekt.messreihen[0]
messreihe_1.load_data_by_version(version="raw")
messreihe_1_short_3600 = messreihe_1.copy_data_by_version(version_source="raw", version_new="short_3600", auto_commit=True)

messreihe_1_short_3600 = messreihe_1.get_data_by_version(version="short_3600")
ergebnis = messreihe_1.limit_time_by_peaks(version="short_3600", duration=3600, auto_commit=True)

print("Ende")
