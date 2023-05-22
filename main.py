# main.py
import treemotion
from treemotion import config, logger, db_manager, Projekt, Messreihe, Messung, Data, WindMessreihe, ValidationManager

logger.setLevel(level="DEBUG")

filename_db_1 = r"TREEMOTION.db"
csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"


db_manager.connect(db_filename=filename_db_1)
validation_manager = ValidationManager()
data_list = Data.load_from_db(load_related_df=False)
data = data_list[0].load_df()
df = data.df
data.describe()

df_val = validation_manager.validate_data(df)


# WindMessreihe.load_from_dwd_online(name="Dörnick_2022", stations_id=6163, auto_commit=True, overwrite=False)
# session = db_manager.get_session()
# windmessreihe = session.query(WindMessreihe).first()



#data_list = Data.load_from_db(id_messung=1, load_related_df=True)
#data = data_list[0]
#wind_df = data.get_wind_df(id_wind_messreihe=1, time_extension_secs=60*60*60)
# projekt.add_filenames(csv_path=csv_path)
# projekt.load_data_from_csv(version="raw", overwrite=False, auto_commit=False)
# data_list = Data.load_from_db()
# data_list = [data.load_df() for data in data_list]
# data_list_update = [data.update_metadata(auto_commit=True) for data in data_list]

# projekt.load_data_by_version(version="raw")
# projekt.copy_data_by_version(version_source="raw", version_new="short_3600", auto_commit=True)

# messreihe_1_short_3600 = messreihe_1.get_data_by_version(version="short_3600")
# ergebnis = messreihe_1.limit_time_by_peaks(version="short_3600", duration=3600, auto_commit=True)
db_manager.disconnect()
