## main.py
import treemotion
from treemotion import config, logger, db_manager, \
    Project, Series, Measurement, Version, WindMeasurement

logger.setLevel(level="DEBUG")

filename_db_1 = r"TREEMOTION_BOSAU_backup - kopie.db"
csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

db_manager.connect(db_filename=filename_db_1)

# project_1 = Project.load_from_db()[0]
# filenames = project_1.add_filenames(csv_path)
#
series = Series.load_from_db(1)[0]
# series_short = series.limit_by_time("copy", start_time="2022-01-29 20:59:59.980071", end_time = "2022-01-29 21:59:59.980071", auto_commit=False)
wind = WindMeasurement.load_from_dwd_online(name="Dornick", station_id=6163, auto_commit=True, overwrite=False)
wind_dfs = series.get_wind_df(version_name="copy", wind_measurement_id=1, time_extension_secs=0)
# versions_opt_time_frame = series.limit_time_by_peaks(version_name="copy", duration=3600, show_peaks=True, auto_commit=True)
#
# measurement_1 = Measurement.load_from_db(1)[0]
# measurement_1.load_from_csv(overwrite=False)
# measurement_2 = Measurement.load_from_db(2)[0]
# measurement_2.load_from_csv(overwrite=False)

# series.copy_versions_by_version_name()

#version_names = series.get_unique_version_names_in_series()

# version_dict = series.version_dict
# version_dict = series.version_dict
# version_dict = series.version_dict

# version_1 = Version.load_from_db()[0]
# copy_1 = version_1.copy(overwrite=False)
# copy_2 = version_1.copy(overwrite=True)
# copy_3 = version_1.copy(overwrite=False)
# copy_4 = version_1.copy(overwrite=True)





#measurements = Measurement.load_from_db(ids=list(range(1, 5)))
# result = [m.load_from_csv() for m in measurements]
# series_1 = Series.load_from_db()[0]
# test = series_1.add_version_list(version_name="raw")
# version_1 = test[0]
# result_1 = Version.create_copy(version_1, copy_version_name="doof")
# result_2 = Version.create_copy(version_1, copy_version_name="doof")
# result_3 = Version.create_copy(version_1, copy_version_name="ja")
#result_2 = version_1.copy(copy_version_name="doof")

# version = Version.load_from_db(1)[0]
# version_new = Version.create_copy(version, "Hallo")
# version_raw = project.get_versions_by_version_name(version_name="raw")


# measurement_list = Measurement.load_from_db(list(range(1, 5)))
# version_list = Version.load_from_db(filter_by={'version_name': "test"})

# returns = project.add_filenames(csv_path)


# projekt.load_data_by_version(version="raw")
# projekt.copy_data_by_version(version_source="raw", version_new="short_3600", dec_auto_commit=True)

# messreihe_1_short_3600 = messreihe_1.get_data_by_version(version="short_3600")
# ergebnis = messreihe_1.limit_time_by_peaks(version="short_3600", duration=3600, dec_auto_commit=True)
# db_manager.disconnect()
print("Ende")
