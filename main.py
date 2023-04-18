import pandas as pd

import treemotion as tms

if __name__ == "__main__":
    print("Start")
    test_dir = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\py_tms_tools\test"
    csv_file = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2023_Kronensicherung_Plesse\020_Daten\TMS\CSV_Messung_001_Plesse_export_2023-03-22_24h\2023-03-22 000000__DatasA000-0000-0007.csv"
    db_file = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\py_tms_tools\test\test.db"

    messung_1 = tms.Messung.read_from_csv(source_path=csv_file, messung_id=1, feedback=True)

    start_time = pd.Timestamp('2023-03-22 07:58:00')
    end_time = pd.Timestamp('2023-03-22 08:00:00')
    messung_1.limit_time(start_time, end_time)

    messung_1.temp_drift_comp(method="temp_drift_comp_emd")

    plot_list = ['East-West-Inclination', 'east_west_inclination_tdc']
    messung_1.plot_data_sub(plot_list)

    plot_list = ['North-South-Inclination', 'north_south_inclination_tdc']
    messung_1.plot_data_sub(plot_list)

    plot_list = ['Absolute-Inclination', 'absolute_inclination_tdc']
    messung_1.plot_data_sub(plot_list)

    plot_list = ['Inclination direction of the tree', 'inclination_direction_tdc']
    messung_1.plot_data_sub_scatter(plot_list)

    test = messung_1.data[plot_list].reset_index()
    print(test.describe())

    summary = pd.DataFrame({'mean': test.mean(), 'median': test.median()})
    print(summary)

    print("Ende")
