from sqlalchemy import types
import configparser
import numpy as np
import scipy
import pandas as pd
from pathlib import Path
import dbi  # importiere das dbi Modul aus dem py_dbi Verzeichnis
from functions import *
import time

import matplotlib.pyplot as plt


class Messung:
    def __init__(self, messung_id: int):

        self.messung_id = messung_id
        self.source_path = Path()
        self.source_file = str()
        self.table_name = str()
        self.data = pd.DataFrame()
        self.datetime_start = None
        self.datetime_end = None
        self.duration = None
        self.length = int()

    def get_table_name(self):
        self.table_name = f"raw_data_{self.messung_id}"

    def update_metadata(self):
        self.length = len(self.data)
        # add start and end time of measurement
        self.datetime_start = min(self.data['Time'])
        self.datetime_end = max(self.data['Time'])
        self.duration = self.datetime_end - self.datetime_start

    @classmethod
    def read_from_csv(cls, source_path: str, messung_id: int, feedback: False):
        start_time = time.time()
        self = cls(messung_id)
        self.messung_id = messung_id
        self.source_path = Path(source_path)
        self.source_file = self.source_path.name
        self.get_table_name()
        self.data = pd.read_csv(self.source_path, sep=";", parse_dates=["Time"], decimal=",")
        # Check if the "Nr" column already exists.
        if "Nr" not in self.data.columns:
            # Add a new column "Nr" with row numbers starting from 1.
            self.data.insert(0, "Nr", pd.Series(range(1, len(self.data) + 1)))
        self.update_metadata()
        if feedback is True:
            print(
                f"Messung.read_from_csv - messung_id: {self.messung_id} - source_file: {self.source_file} - time: {time.time() - start_time:.2f} sec")
        return self

    @classmethod
    def read_from_db(cls, source_path: str, messung_id: int, feedback=False):
        start_time = time.time()
        self = cls(messung_id)
        self.messung_id = messung_id
        self.source_path = Path(source_path)
        self.source_file = self.source_path.name
        self.get_table_name()
        self.data = dbi.read_df(self.source_path, self.table_name)
        self.update_metadata()
        if feedback is True:
            print(
                f"Messung.read_from_db - messung_id: {self.messung_id} - source_file: {self.source_file} - time: {time.time() - start_time:.2f} sec")
        return self

    def write_to_csv(self, output_dir, bz2=False, feedback=False):
        start_time = time.time()
        if bz2 is True:
            file_ext = ".csv.bz2"
        else:
            file_ext = ".csv"

        file_path = Path(output_dir) / f"{self.table_name}{file_ext}"
        self.data.to_csv(file_path, index=False, sep=";", decimal=",")
        if feedback is True:
            print(
                f"Messung.write_to_csv - messung_id: {self.messung_id} - new_file: {file_path} - time: {time.time() - start_time:.2f} sec")

    def write_to_db(self, db_path, feedback=False):
        start_time = time.time()
        dtype_dict = self.read_config_dtypes()
        dbi.write_df(db_path, table_name=self.table_name, data_df=self.data, dtype_dict=dtype_dict)
        if feedback is True:
            print(
                f"Messung.write_to_db - messung_id: {self.messung_id} - table_name: {self.table_name} - time: {time.time() - start_time:.2f} sec")

    @staticmethod
    def read_config_dtypes(config_file='config.txt'):
        # Read the data types for the columns of the new table from a config file.
        config = configparser.ConfigParser(interpolation=None, strict=True)
        config.read(config_file)
        dtype_dict = {}
        for key, value in config.items('data_types'):
            dtype_dict[key] = getattr(types, value)
        return dtype_dict

    def limit_time(self, start_time, end_time):
        self.data = limit_data_by_time(self.data, time_col="Time", start_time=start_time, end_time=end_time)
        self.update_metadata()

    def random_sample(self, n):

        self.data = random_sample(self.data, n=n)
        self.update_metadata()

    def temp_drift_comp(self, method="temp_drift_comp_lin_reg"):
        time = self.data['Time']
        temp = self.data['Temperature']
        x = self.data['East-West-Inclination']
        y = self.data['North-South-Inclination']
        size = 1000
        sample_rate  = 20
        freq_range = (0.05, 2, 128) # why 128??

        methods = {
            "temp_drift_comp_lin_reg": lambda x: temp_drift_comp_lin_reg(x, temp),
            "temp_drift_comp_lin_reg_2": lambda x: temp_drift_comp_lin_reg_2(x, temp),
            "temp_drift_comp_mov_avg": lambda x: temp_drift_comp_mov_avg(x, window_size=size),
            "temp_drift_comp_emd": lambda x: temp_drift_comp_emd(x, sample_rate, freq_range, plot=True),
            "temp_drift_comp_emd_2": lambda x: temp_drift_comp_emd_2(x, sample_rate, freq_range, plot=True),
        }

        if method in methods:
            x = methods[method](x)
            y = methods[method](y)
        else:
            raise ValueError(f"Invalid method: {method}")

        z = get_absolute_inclination(x, y)
        d = get_inclination_direction(x, y)

        self.data['east_west_inclination_tdc'] = x
        self.data['north_south_inclination_tdc'] = y
        self.data['absolute_inclination_tdc'] = z
        self.data['inclination_direction_tdc'] = d

    def plot_data(self, y_cols):
        fig, ax = plt.subplots()
        x = self.data["Time"]
        for col in y_cols:
            y = self.data[col]
            ax.plot(x, y, label=col)
        ax.set_xlabel("Time")
        ax.legend()
        plt.show()

    def plot_data_sub(self, y_cols):
        num_plots = len(y_cols)
        fig, axs = plt.subplots(num_plots, 1, figsize=(8, num_plots * 4))
        x = self.data["Time"]
        for i, col in enumerate(y_cols):
            y = self.data[col]
            axs[i].plot(x, y, label=col)
            axs[i].set_xlabel("Time")
            axs[i].legend()
        plt.show()

    def plot_data_sub_scatter(self, y_cols):
        num_plots = len(y_cols)
        fig, axs = plt.subplots(num_plots, 1, figsize=(8, num_plots * 4))
        x = self.data["Time"]
        for i, col in enumerate(y_cols):
            y = self.data[col]
            axs[i].scatter(x, y, label=col, s=0.25)
            axs[i].set_xlabel("Time")
            axs[i].legend()
        plt.show()


if __name__ == "__main__":
    print("Start")
    test_dir = r"C:\Users\mail\Meine Ablage\OB_GDrive_Kyellsen\006_Tools\py_tms_tools\test"
    csv_file = r"C:\Users\mail\Meine Ablage\OB_GDrive_Kyellsen\005_Projekte\2023_Kronensicherung_Plesse\020_Daten\TMS\CSV_Messung_001_Plesse_export_2023-03-22_24h\2023-03-22 000000__DatasA000-0000-0007.csv"
    db_file = r"C:\Users\mail\Meine Ablage\OB_GDrive_Kyellsen\006_Tools\py_tms_tools\test\test.db"

    messung_1 = Messung.read_from_csv(source_path=csv_file, messung_id=1, feedback=True)

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
