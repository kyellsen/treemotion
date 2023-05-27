## main.py
import treemotion
from treemotion import config, logger, db_manager, Project, Series, Measurement, Version, \
    WindMeasurement , SeriesVersion# , ValidationManager

logger.setLevel(level="DEBUG")

filename_db_1 = r"TREEMOTION_BOSAU_backup - kopie.db"
csv_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2022_Bosau\020_Daten"

db_manager.connect(db_filename=filename_db_1)
projekt_1 = Project.load_from_db()
series_ls = Series.load_from_db(1)

version = Version.load_from_db(1)[0]
version_new = Version.create_new_version(version, "Hallo")
# project = Project.load_from_db(ids=1, get_tms_df=False)
# version_raw = project.get_version(version_name="raw")
# test = SeriesVersion(series_version_id=1, series_id=1, version_name="raw", get_tms_df=False)


#measurement_list = Measurement.load_from_db(list(range(1, 5)))
#version_list = Version.load_from_db(filter_by={'version_name': "test"})

#returns = project.add_filenames(csv_path)


# projekt.load_data_by_version(version="raw")
# projekt.copy_data_by_version(version_source="raw", version_new="short_3600", dec_auto_commit=True)

# messreihe_1_short_3600 = messreihe_1.get_data_by_version(version="short_3600")
# ergebnis = messreihe_1.limit_time_by_peaks(version="short_3600", duration=3600, dec_auto_commit=True)
db_manager.disconnect()
