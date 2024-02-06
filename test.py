from pathlib import Path
import numpy as np
import pandas as pd
import sys
from typing import List, Tuple
from kj_logger import get_logger

import treemotion as tms
from treemotion import Project, Series, Measurement, MeasurementVersion, DataTMS, DataMerge
from treemotion import CrownMotionSimilarity

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

    #DATABASE_MANAGER.duplicate(database_path=str(source_db))
    DATABASE_MANAGER.connect(db_name=str(db_name))

    project = DATABASE_MANAGER.load(Project, 1)[0]
    series: Series = DATABASE_MANAGER.load(Series, 1)[0]
    m_1: Measurement = series.measurement[0]
    mv_1: MeasurementVersion = m_1.measurement_version[0]
    md_1: DataMerge = mv_1.data_merge

    # size_in_bytes = sys.getsizeof(md_1)
    # size_in_gb = size_in_bytes / 1_073_741_824
    # print(f"Speicherbedarf für 'my_list': {size_in_gb} GB")

    project.method_for_all_children("add_filenames", csv_path=csv_path)

    # filename_wind = 'produkt_zehn_min_ff_20200101_20221231_06163.txt'
    # filename_wind_extreme = 'produkt_zehn_min_fx_20200101_20221231_06163.txt'

    # project.method_for_all_children("add_wind_station", "06163", filename_wind=filename_wind,
    #                                 filename_wind_extreme=filename_wind_extreme, update_existing=False)
    # series.add_wind_station("06163", filename_wind=filename_wind, filename_wind_extreme=filename_wind_extreme,
    #                         update_existing=False)
    #
    # series.method_for_all_of_class("Measurement", "load_from_csv", measurement_version_name="raw",
    #                                update_existing=False)
    # series.method_for_all_of_class("Measurement", "load_from_csv", measurement_version_name="edit_001",
    #                                update_existing=False)

    # series.calc_optimal_shift_median(measurement_version_name="raw")
    # series.method_for_all_of_class("MeasurementVersion", "add_data_merge", update_existing=False)
    # series.method_for_all_of_class("MeasurementVersion", "plot_shift_sync_wind_tms", mode="median")
    # result, peaks = series.cut_time_by_peaks(measurement_version_name="edit_001", inplace=True, auto_commit=True)

    mv_list: List[MeasurementVersion] = series.get_measurement_version_by_filter(
        filter_dict={'measurement_version_name': "edit_001"})

    mv_1: MeasurementVersion = mv_list[0]

    # for mv in mv_list:
    #     mv.data_merge.correct_tms_data(method="linear", freq_filter="butter_lowpass", rotation="rotation_pca", inplace=True, auto_commit=True)
    #
    # cms_list: List[CrownMotionSimilarity] = CrownMotionSimilarity.create_all_cms(series_id=1, measurement_version_name="edit_001")
    #
    # cms_1 = cms_list[0]
    # cms_1.analyse_similarity()
    # for cms in cms_list:
    #     cms.plot_filtered_trunk_data()
    #
    # results = []  # Initialize list to store result dictionaries
    #
    # for cms in cms_list:
    #     try:
    #         corr, p_value, rmse, mae, tree_cable_typ = cms.analyse_similarity()
    #         results.append({
    #             'corr': corr,
    #             'p_value': p_value,
    #             'rmse': rmse,
    #             'mae': mae,
    #             'tree_cable_typ': tree_cable_typ
    #         })
    #     except Exception as e:
    #         print(f"Error calculating optimal get_shifted_trunk_data for {cms}: {e}")
    #
    # # Convert results to DataFrame
    # df = pd.DataFrame(results)
    #
    # df['tree_cable_typ'] = df['tree_cable_typ'].astype('category')
    #
    # df.to_csv(working_directory/'export/cms.csv')


    def plot_cable_typ(data: pd.DataFrame):
        import matplotlib.pyplot as plt
        import seaborn as sns

        # Einstellungen für die Darstellung
        sns.set(style="whitegrid")

        # Erstellung von Plots für jeden Parameter ('corr', 'rmse', 'mae'), gruppiert nach 'tree_cable_typ'
        fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=False)

        # Korrelation
        sns.boxplot(x='tree_cable_typ', y='corr', data=data, ax=axes[0])
        axes[0].set_title('Korrelation nach Kabeltyp')
        axes[0].set_xlabel('Baumkabeltyp')
        axes[0].set_ylabel('Korrelation')

        # Root Mean Square Error
        sns.boxplot(x='tree_cable_typ', y='rmse', data=data, ax=axes[1])
        axes[1].set_title('Root Mean Square Error nach Kabeltyp')
        axes[1].set_xlabel('Baumkabeltyp')
        axes[1].set_ylabel('RMSE')

        # Mean Absolute Error
        sns.boxplot(x='tree_cable_typ', y='mae', data=data, ax=axes[2])
        axes[2].set_title('Mean Absolute Error nach Kabeltyp')
        axes[2].set_xlabel('Baumkabeltyp')
        axes[2].set_ylabel('MAE')

        plt.tight_layout()
        plt.show()


    def analyze_groups(data: pd.DataFrame):
        from scipy.stats import ttest_ind
        # Gruppierung der Daten nach 'tree_cable_typ' und Berechnung der statistischen Kennzahlen für jede Gruppe
        grouped_stats = data.groupby('tree_cable_typ').agg(['mean', 'std'])

        # Vorbereitung der Daten für t-Tests
        dynamisch = data[data['tree_cable_typ'] == 'dynamisch']
        free = data[data['tree_cable_typ'] == 'free']

        # Durchführung der t-Tests
        ttest_results = {}
        for col in ['corr', 'rmse', 'mae']:
            t_stat, p_value = ttest_ind(dynamisch[col], free[col], equal_var=True)
            ttest_results[col] = {'t_stat': t_stat, 'p_value': p_value}

        return grouped_stats, ttest_results

    print(analyze_groups(df))

    plot_cable_typ(df)

    # data_tms: DataTMS = measurement_version.data_tms
    # data_merge: DataMerge = measurement_version.data_merge

    # start_time = '2022-01-29T15:00:00'
    # end_time = '2022-01-29T20:00:00'


    # tms_data.to_csv(working_directory/'export/tms_data.csv')
    # wind_data.to_csv(working_directory/'export/wind_data.csv')
    # mv.add_data_merge(update_existing=False)

    # mv_list = DATABASE_MANAGER.load(MeasurementVersion)

    # DATABASE_MANAGER.disconnect()
