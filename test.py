from pathlib import Path
import numpy as np
import pandas as pd
import sys
from typing import List, Tuple
from kj_logger import get_logger

import treemotion as tms
from treemotion import Project, Series, Measurement, MeasurementVersion, DataTMS, DataMerge, DataLS3

if __name__ == "__main__":
    main_path = Path(r"C:\kyellsen\005_Projekte\2022_Bosau")
    analyse_name = r"2022_Bosau_2023-12-11"
    data_path = main_path / "021_Daten_Clean"  # Für alle Daten-Importe des Projektes gemeinsam
    working_directory = main_path / "030_Analysen" / analyse_name / "working_directory"  # Für alle Daten-Exporte des Projektes gemeinsam
    db_name = "TREEMOTION_Bosau_2023-05-12.db"
    source_db = data_path / db_name
    csv_path = r"C:\kyellsen\005_Projekte\2022_Bosau\020_Daten"

    CONFIG, LOG_MANAGER, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER = tms.setup(
        working_directory=str(working_directory), log_level="DEBUG")

    DATABASE_MANAGER.duplicate(database_path=str(source_db))
    DATABASE_MANAGER.connect(db_name=str(db_name))

    project = DATABASE_MANAGER.load(Project, 1)[0]
    project.method_for_all_children("add_filenames", csv_path=csv_path)

    filename_wind = 'produkt_zehn_min_ff_20200101_20221231_06163.txt'
    filename_wind_extreme = 'produkt_zehn_min_fx_20200101_20221231_06163.txt'
    series: Series = DATABASE_MANAGER.load(Series, 1)[0]
    series.add_wind_station("06163", filename_wind=filename_wind, filename_wind_extreme=filename_wind_extreme,
                            update_existing=True)
    project.method_for_all_children("add_wind_station", "06163")
    project.method_for_all_children("add_wind_station", "06163", filename_wind=filename_wind,
                                    filename_wind_extreme=filename_wind_extreme, update_existing=True)

    series.method_for_all_of_class("Measurement", "load_from_csv", update_existing=True)

    measurement: Measurement = series.measurement[0]
    measurement.load_from_csv(update_existing=True)
    measurement_version = measurement.measurement_version[0]

    shift = series.calc_optimal_shift_median()
    measurement_version.add_data_merge(update_existing=True)

    series.method_for_all_of_class("MeasurementVersion", "add_data_merge", update_existing=True)
    series.method_for_all_of_class("MeasurementVersion", "plot_shift_sync_wind_tms", mode="median")

    data_tms: DataTMS = measurement_version.data_tms
    data_merge: DataMerge = measurement_version.data_merge
    start_time = '2022-01-29T19:30:00'
    end_time = '2022-01-29T20:00:00'
    mv_list: List[MeasurementVersion] = DATABASE_MANAGER.load(MeasurementVersion)

    for mv in mv_list:
        mv.data_tms.time_cut(start_time, end_time, inplace=True, auto_commit=True)
        mv.data_tms.plot_compare_tms_tempdrift()
        mv.data_merge.time_cut(start_time, end_time, inplace=True, auto_commit=True)
        mv.data_merge.plot_compare_tms_tempdrift()


    # tms_data.to_csv(working_directory/'export/tms_data.csv')
    # wind_data.to_csv(working_directory/'export/wind_data.csv')
    # mv.add_data_merge(update_existing=False)

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
