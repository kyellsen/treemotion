import treemotion as tms
from pathlib import Path
import sys

from treemotion.classes import Measurement, Series, Project, MeasurementVersion, DataWindStation, DataTMS, DataMerge

# Beispiel für die Verwendung
if __name__ == "__main__":
    # Main
    main_path = Path(r"C:\kyellsen\005_Projekte\2022_Bosau")
    analyse_name = r"2022_Bosau_2023-12-11"
    data_path = main_path / "021_Daten_Clean"  # Für alle Daten-Importe des Projektes gemeinsam
    working_directory = main_path / "030_Analysen" / analyse_name / "working_directory"  # Für alle Daten-Exporte des Projektes gemeinsam
    db_name = "TREEMOTION_Bosau_2023-05-12.db"
    source_db = data_path / db_name
    csv_path = r"C:\kyellsen\005_Projekte\2022_Bosau\020_Daten"

    tms_working_directory = working_directory / 'tms'
    CONFIG, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER = tms.setup(working_directory=str(tms_working_directory),
                                                                     log_level="DEBUG")

    # DATA_MANAGER.cleanup_data_space()
    DATABASE_MANAGER.duplicate(database_path=str(source_db))
    DATABASE_MANAGER.connect(db_name=str(db_name))
    # sys.exit()
    project = DATABASE_MANAGER.load(Project, 1)[0]
    project.method_for_all_children("add_filenames", csv_path=csv_path)
    filename_wind = 'produkt_zehn_min_ff_20200101_20221231_06163.txt'
    filename_wind_extreme = 'produkt_zehn_min_fx_20200101_20221231_06163.txt'
    series: Series = DATABASE_MANAGER.load(Series, 1)[0]
    series.add_wind_station("06163", filename_wind=filename_wind, filename_wind_extreme=filename_wind_extreme,
                            update_existing=True)

    # project.method_for_all_children("add_wind_station", "06163")
    # project.method_for_all_children("add_wind_station", "06163", filename_wind=filename_wind,
    #                                 filename_wind_extreme=filename_wind_extreme, update_existing=True)
    # project.method_for_all_children("add_wind_station", "06163", filename_wind=filename_wind,
    #                                 filename_wind_extreme=filename_wind_extreme, update_existing=False)
    # project.method_for_all_children("add_wind_station", "06163", filename_wind=filename_wind,
    #                                 filename_wind_extreme=filename_wind_extreme, update_existing=True)
    measurement = series.measurement[0]
    measurement.load_from_csv(update_existing=False)
    mv: MeasurementVersion = measurement.measurement_version[0]
    data = mv.data_tms.data
    mv.add_data_merge(update_existing=False)

    # series.method_for_all_of_class("Measurement", "load_from_csv", update_existing=False)
    # series.method_for_all_of_class("Measurement", "load_from_csv", update_existing=False)
    # series.method_for_all_of_class("Measurement", "load_from_csv", update_existing=True)
    #
    # DataTMS_list = DATABASE_MANAGER.load(DataTMS)
    # mv_list = DATABASE_MANAGER.load(MeasurementVersion)
    # series.method_for_all_of_class("MeasurementVersion", "add_wind_from_station", update_existing=False)
    # series.method_for_all_of_class("MeasurementVersion", "add_wind_from_station", update_existing=False)
    # series.method_for_all_of_class("MeasurementVersion", "add_wind_from_station", update_existing=True)
    #
    # peaks = [DataTMS.peaks for DataTMS in DataTMS_list]

    # DATABASE_MANAGER.disconnect()
