import configparser
import time
from pathlib import Path
import os
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import types

from treemotion.utilities import basics
from treemotion.utilities import dbi
from treemotion.utilities import tempdrift


class Data:
    def __init__(self, type: str):
        type = str()
        data = pd.DataFrame
        datetime_start: None
        datetime_end: None
        duration: None
        length: None

    def update_metadata(self):
        # add start and end time of measurement
        self.datetime_start = min(self.data['Time'])
        self.datetime_end = max(self.data['Time'])
        self.duration = self.datetime_end - self.datetime_start
        self.length = len(self.data)

class Messung:
    def __init__(self, messung_id: int):

        self.messung_id = messung_id
        self.source_path = Path()
        self.source_file = str()
        self.table_name = str()
        self.data_raw = Data('raw')
        self.data = Data('processed')

    def get_table_name(self):
        self.table_name = f"data_{self.messung_id}"



    @classmethod
    def read_from_csv(cls, source_path: str, messung_id: int, feedback: False):
        start_time = time.time()
        self = cls(messung_id)
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
    def read_config_dtypes():
        # Read the data types for the columns of the new table from a config file.
        config = configparser.ConfigParser(interpolation=None, strict=True)

        config_path = Path(__file__).resolve().parent.parent / "config.txt"
        config.read(str(config_path))
        dtype_dict = {}
        for key, value in config.items('data_types'):
            dtype_dict[key] = getattr(types, value)
        return dtype_dict

    def limit_time(self, start_time, end_time):
        self.data = basics.limit_by_time(self.data, time_col="Time", start_time=start_time, end_time=end_time)
        self.update_metadata()

    def random_sample(self, n):
        self.data = self.data.sample(n)
        self.update_metadata()

    def temp_drift_comp(self, method="temp_drift_comp_lin_reg"):
        temp = self.data['Temperature']
        x = self.data['East-West-Inclination']
        y = self.data['North-South-Inclination']
        window_size = 1000
        sample_rate = 20
        freq_range = (0.05, 2, 128) # why 128??

        # data = x or y
        methods = {
            "temp_drift_comp_lin_reg": lambda data: tempdrift.temp_drift_comp_lin_reg(data, temp),
            "temp_drift_comp_lin_reg_2": lambda data: tempdrift.temp_drift_comp_lin_reg_2(data, temp),
            "temp_drift_comp_mov_avg": lambda data: tempdrift.temp_drift_comp_mov_avg(data, window_size),
            "temp_drift_comp_emd": lambda data: tempdrift.temp_drift_comp_emd(data, sample_rate, freq_range),
        }

        if method in methods:
            x = methods[method](x)
            y = methods[method](y)
        else:
            raise ValueError(f"Invalid method: {method}")

        self.data['east_west_inclination_tdc'] = x
        self.data['north_south_inclination_tdc'] = y
        self.data['absolute_inclination_tdc'] = basics.get_absolute_inclination(x, y)
        self.data['inclination_direction_tdc'] = basics.get_inclination_direction(x, y)

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
