from pathlib import Path
import numpy as np
import pandas as pd
import sys
from typing import List, Tuple, Dict

import matplotlib.pyplot as plt
import seaborn as sns

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

    project.method_for_all_children("add_filenames", csv_path=csv_path)

    filename_wind = 'produkt_zehn_min_ff_20200101_20221231_06163.txt'
    filename_wind_extreme = 'produkt_zehn_min_fx_20200101_20221231_06163.txt'

    # TESTING FOR ONE MEASUREMENT
    # if False:
    #     m_1: Measurement = series.measurement[5]
    #     mv_1 = m_1.load_from_csv(measurement_version_name="rotate", update_existing=True)
    #     mv_1.add_data_merge(update_existing=True)
    #     start_time = '2022-01-29T19:30:00'
    #     end_time = '2022-01-29T19:35:00'
    #
    #     mv_1.data_merge.cut_by_time(start_time, end_time, inplace=True, auto_commit=True)
    #
    #     mv_1.data_merge.plot_compare_correct_tms_data_methods()
    #     mv_1: MeasurementVersion = mv_list[0]
    #     mv_1.data_merge.plot_compare_correct_tms_data_methods()

    # size_in_bytes = sys.getsizeof(md_1)
    # size_in_gb = size_in_bytes / 1_073_741_824
    # print(f"Speicherbedarf für 'my_list': {size_in_gb} GB")

    # project.method_for_all_children("add_wind_station", "06163", filename_wind=filename_wind,
    #                                filename_wind_extreme=filename_wind_extreme, update_existing=False)
    series.add_wind_station("06163", filename_wind=filename_wind, filename_wind_extreme=filename_wind_extreme,
                            update_existing=True)

    # series.method_for_all_of_class("Measurement", "load_from_csv", measurement_version_name="raw",
    #                                update_existing=True)
    series.method_for_all_of_class("Measurement", "load_from_csv", measurement_version_name="rotate",
                                   update_existing=True)

    series.calc_optimal_shift_median(measurement_version_name="rotate")
    #series.method_for_all_of_class("MeasurementVersion", "add_data_merge", update_existing=True)
    #series.method_for_all_of_class("MeasurementVersion", "plot_shift_sync_wind_tms", mode="median")


    mv_list: List[MeasurementVersion] = series.get_measurement_version_by_filter(
        filter_dict={'measurement_version_name': "rotate"})
    for mv in mv_list:
        mv.add_data_merge(update_existing=True)
    result, peaks = series.cut_time_by_peaks(measurement_version_name="rotate", inplace=True, auto_commit=True)

    for mv in mv_list:
        mv.data_merge.plot_compare_correct_tms_data_methods()

    for mv in mv_list:
        mv.data_merge.correct_tms_data(method="linear", freq_filter="butter_lowpass", rotation="rotate_pca", inplace=True, auto_commit=True)

    cms_list: List[CrownMotionSimilarity] = CrownMotionSimilarity.create_all_cms(series_id=1, measurement_version_name="rotate")

    cms_list = cms_list[0:8]
    for cms in cms_list:
        cms.plot_shifted_data()

    for cms in cms_list:
        cms.plot_analyse_similarity()

    all_dicts = []

    for cms in cms_list:
        try:
            data = cms.analyse_similarity()
            # Füge den DataFrame zur Liste hinzu.
            all_dicts.append(data)
        except Exception as e:
            print(f"Error analyse_similarity for {cms}: {e}")
    df_all = pd.DataFrame(all_dicts).astype({'tree_cable_type': 'category'})
    df_all.to_csv(working_directory/'export/cms.csv')


    def analyze_groups(data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Dict[str, float]]]:
        from scipy.stats import ttest_ind
        # Prepare the data by averaging over the axes for each measurement
        averaged_data = data.groupby(['cms_id', 'tree_cable_type'], observed=True).mean().reset_index()

        # Recalculate grouped statistics with the averaged data
        grouped_stats = averaged_data.groupby(by=['tree_cable_type']).agg(['mean', 'std'])

        # Prepare the data for t-Tests
        dynamisch = averaged_data[averaged_data['tree_cable_type'] == 'dynamisch']
        free = averaged_data[averaged_data['tree_cable_type'] == 'free']

        # Initialize a dictionary to hold t-test results
        ttest_results = {}

        # Perform t-Tests for each metric
        for col in ['pearson_r', 'rmse', 'mae']:
            # Extracting the relevant metrics for each group
            dynamisch_metrics = dynamisch[col]
            free_metrics = free[col]

            # Conducting the t-Test with NaN handling
            t_stat, p_value = ttest_ind(dynamisch_metrics, free_metrics, equal_var=False, nan_policy='omit')
            ttest_results[col] = {'t_stat': t_stat, 'p_value': p_value}

        return grouped_stats, ttest_results


    def plot_cable_type(data: pd.DataFrame):
        import seaborn as sns
        import matplotlib.pyplot as plt

        sns.set(style="whitegrid")

        metrics = ['pearson_r', 'rmse', 'mae']
        titles = ['Correlation by Cable Type', 'Root Mean Square Error by Cable Type',
                  'Mean Absolute Error by Cable Type']
        y_labels = ['Correlation', 'RMSE', 'MAE']

        fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=False)

        for i, (metric, title, y_label) in enumerate(zip(metrics, titles, y_labels)):
            sns.boxplot(x='tree_cable_type', y=metric, data=data, ax=axes[i])
            axes[i].set_title(title)
            axes[i].set_xlabel('Tree Cable Type')
            axes[i].set_ylabel(y_label)

        plt.tight_layout()
        plt.show()


    grouped_stats, ttest_results = analyze_groups(df_all)

    print(grouped_stats)
    print(ttest_results)

    plot_cable_type(df_all)

