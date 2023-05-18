# main.py
import treemotion
from treemotion import db_manager, Projekt, Messreihe, Messung, Data

print("Start")

path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION.db"
csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

db_manager.connect(path_db_1)
projekt_list = Projekt.load_from_db()
projekt = projekt_list[0]
#projekt.add_filenames(csv_path=csv_path)
projekt.load_data_from_csv(version="raw", overwrite=False, auto_commit=True)
messreihe = projekt.messreihen[0]

messreihe.copy_version(version_source="raw", version_new="short_1", auto_commit=True)
messreihe.load_data_by_version(version="short_1")

data_list = messreihe.get_data_by_version("short_1")
random_list = [data.random_sample(n=1000) for data in data_list]

data_version_short_1_list = projekt.get_data_by_version("short_1")


print("Ende")
