# main.py
import treemotion
from treemotion import config, logger, db_manager, Project, Series, Measurement, Version, \
    WindMeasurement  # , ValidationManager

logger.setLevel(level="DEBUG")

filename_db_1 = r"TREEMOTION_BOSAU.db"
csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

db_manager.connect(db_filename=filename_db_1)
project_1 = Project.load_from_db()[0]
series_list = Series.load_from_db(ids=[1])
# version_list = Version.load_from_db(get_tms_df=True)
#project_1 = project_1.load_from_csv(overwrite=False)
# version_list = Version.load_from_db(version_id=list(range(5)), get_tms_df=True)
# measurement_1.load_from_csv(overwrite=True)


# series_1 = Measurement.load_from_db(measurement_id=list(range(5)))[0]
# series_1.load_from_csv()

# WindMeasurement.load_from_dwd_online(name="Dornick", station_id=6163, dec_auto_commit=True, overwrite=False)
# session = db_manager.get_session()
# windmessreihe = session.query(WindMeasurement).first()
# validation_manager = ValidationManager()
# data_list = Version.load_from_db(load_related_df=False)
# data = data_list[0].load_df()
# df = data.df
# data.describe()
#
# df_val = validation_manager.validate_data(df)


# WindMessreihe.load_from_dwd_online(name="Dörnick_2022", stations_id=6163, dec_auto_commit=True, overwrite=False)
# session = db_manager.get_session()
# windmessreihe = session.query(WindMessreihe).first()


# data_list = Data.load_from_db(id_messung=1, load_related_df=True)
# data = data_list[0]
# wind_df = data.get_wind_df(id_wind_messreihe=1, time_extension_secs=60*60*60)
# projekt.add_filenames(csv_path=csv_path)
# projekt.load_from_csv(version="raw", overwrite=False, dec_auto_commit=False)
# data_list = Data.load_from_db()
# data_list = [data.load_df() for data in data_list]
# data_list_update = [data.update_metadata(dec_auto_commit=True) for data in data_list]

# projekt.load_data_by_version(version="raw")
# projekt.copy_data_by_version(version_source="raw", version_new="short_3600", dec_auto_commit=True)

# messreihe_1_short_3600 = messreihe_1.get_data_by_version(version="short_3600")
# ergebnis = messreihe_1.limit_time_by_peaks(version="short_3600", duration=3600, dec_auto_commit=True)
db_manager.disconnect()
