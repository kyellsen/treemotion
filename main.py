# main.py
import treemotion
from treemotion import db_manager, Projekt, Messreihe, Messung, Data, WindMessreihe

print("Start")

path_db_1 = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\projekt_test_data\TREEMOTION.db"
csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"
wind_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten\Wind\treemotion_wind_auto"

db_manager.connect(path_db_1)

# test = WindMessreihe.load_from_dwd_online(name="Dornick", stations_id=6163, folder_path=wind_path, overwrite=True,
#                                           auto_commit=True)

projekt = Projekt.load_from_db()[0]
projekt.add_filenames(csv_path=csv_path)
projekt.load_data_from_csv(version="raw", overwrite=False, auto_commit=False)
data_list = Data.load_from_db()
data_list = [data.load_df() for data in data_list]
data_list_update = [data.update_metadata(auto_commit=True) for data in data_list]

# projekt.load_data_by_version(version="raw")
# projekt.copy_data_by_version(version_source="raw", version_new="short_3600", auto_commit=True)

# messreihe_1_short_3600 = messreihe_1.get_data_by_version(version="short_3600")
# ergebnis = messreihe_1.limit_time_by_peaks(version="short_3600", duration=3600, auto_commit=True)
db_manager.disconnect()


print("Ende")
