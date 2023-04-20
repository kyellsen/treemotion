import configparser
import time
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import types

from treemotion.utilities import basics
from treemotion.utilities import dbi
from treemotion.utilities import tempdrift


class Messung:
    def __init__(self, id_messung: int):
        self.id_messung = id_messung
        self.sensor = None
        self.source_path = Path()
        self.source_file = str()
        self.data = pd.DataFrame()
        self.metadata = {}
        self.raw_data = pd.DataFrame()
        self.raw_metadata = {}

    def update_metadata(self):
        self.metadata['datetime_start'] = self.data['Time'].min()
        self.metadata['datetime_end'] = self.data['Time'].max()
        self.metadata['duration'] = self.metadata['datetime_end'] - self.metadata['datetime_start']
        self.metadata['length'] = len(self.data)

    @classmethod
    def read_from_csv(cls, db_path: str, id_messung: int, feedback: False):
        start_time = time.time()
        obj = cls(id_messung)
        obj.source_path = Path(db_path)
        obj.source_file = obj.source_path.name
        obj.data = pd.read_csv(obj.source_path, sep=";", parse_dates=["Time"], decimal=",")
        obj.update_metadata()
        obj.raw_data = obj.data.copy()
        obj.raw_metadata = obj.metadata.copy()

        if feedback is True:
            print(
                f"Messung.read_from_csv - id_messung: {obj.id_messung} - source_file: {obj.source_file} - time: {time.time() - start_time:.2f} sec")
        return obj

    @classmethod
    def read_from_db(cls, csv_path: str, id_messung: int, add_name="", feedback=False):
        start_time = time.time()
        obj = cls(id_messung)
        obj.source_path = Path(csv_path)
        obj.source_file = obj.source_path.name
        table_name = obj.get_table_name(add_name=add_name)
        obj.data = dbi.read_df(obj.source_path, table_name)
        obj.update_metadata()
        obj.raw_data = obj.data.copy()
        obj.raw_metadata = obj.metadata.copy()

        if feedback is True:
            print(
                f"Messung.read_from_db - id_messung: {obj.id_messung} - source_file: {obj.source_file} - time: {time.time() - start_time:.2f} sec")
        return obj

    def get_table_name(self, add_name):

        return f"{add_name}_{self.id_messung}"

    def write_to_csv(self, output_dir, add_name="data", raw_data=False, bz2=False, feedback=False):
        start_time = time.time()
        if bz2 is True:
            file_ext = ".csv.bz2"
        else:
            file_ext = ".csv"
        if raw_data is True:
            data = self.raw_data
        else:
            data = self.data
        table_name = self.get_table_name(add_name)
        file_path = Path(output_dir) / f"{table_name}{file_ext}"
        data.to_csv(file_path, index=False, sep=";", decimal=",")
        if feedback is True:
            print(
                f"Messung.write_to_csv - id_messung: {self.id_messung} - new_file: {file_path} - raw_data: {raw_data} - time: {time.time() - start_time:.2f} sec")

    def write_to_db(self, db_path, add_name="data", raw_data=False, feedback=False):
        start_time = time.time()
        table_name = self.get_table_name(add_name)
        dtype_dict = self.read_config_dtypes()
        if raw_data is True:
            data = self.raw_data
        else:
            data = self.data
        dbi.write_df(db_path, table_name=table_name, data=data, dtypes=dtype_dict)
        if feedback is True:
            print(
                f"Messung.write_to_db - id_messung: {self.id_messung} - table_name: {table_name} - raw_data: {raw_data} time: {time.time() - start_time:.2f} sec")

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

    def temp_drift_comp(self, method="emd_hht", overwrite=True, sample_rate=20, window_size=1000,
                        freq_range=(0.05, 2, 128), feedback=False):  # 128 is used because ...
        """
        Compensate the temperature drift in the measurements using the specified method.

        :param method: The method to use for temperature drift compensation.
        :param overwrite: Whether to overwrite the original data or create new columns.
        :param sample_rate: The sample rate of the data (in Hz).
        :param window_size: The window size for the moving average method.
        :param freq_range: The frequency range for the EMD-HHT method.
        :param feedback: Show result and runtime
        """
        start_time = time.time()
        temp = self.data['Temperature']

        methods = {
            "lin_reg": lambda data: tempdrift.temp_drift_comp_lin_reg(data, temp),
            "lin_reg_2": lambda data: tempdrift.temp_drift_comp_lin_reg_2(data, temp),
            "mov_avg": lambda data: tempdrift.temp_drift_comp_mov_avg(data, window_size),
            "emd_hht": lambda data: tempdrift.temp_drift_comp_emd(data, sample_rate, freq_range),
        }

        if method in methods:
            x = methods[method](self.data['East-West-Inclination'])
            y = methods[method](self.data['North-South-Inclination'])
        else:
            raise ValueError(f"Invalid method for temp_drift_comp: {method}")

        suffix = "" if overwrite else " - new"

        self.data[f'East-West-Inclination - drift compensated{suffix}'] = x
        self.data[f'North-South-Inclination - drift compensated{suffix}'] = y
        self.data[f'Absolute-Inclination - drift compensated{suffix}'] = basics.get_absolute_inclination(x, y)
        self.data[f'Inclination direction of the tree - drift compensated{suffix}'] = basics.get_inclination_direction(
            x, y)
        if feedback is True:
            print(
                f"Messung.temp_drift_comp - id_messung: {self.id_messung} -  time: {time.time() - start_time:.2f} sec")


    def plot_data(self, y_cols):
        """
        Plots the specified columns of data against time.

        Args:
            y_cols (list of str): The names of the columns to plot on the y-axis.

        Returns:
            None.

        Raises:
            KeyError: If any of the specified column names are not in the data.
        """
        fig, ax = plt.subplots()
        x = self.data["Time"]
        for col in y_cols:
            try:
                y = self.data[col]
            except KeyError:
                raise KeyError(f"Column '{col}' not found in data.")
            ax.plot(x, y, label=col)
        ax.set_xlabel("Time")
        ax.legend()
        plt.show()

    def plot_data_sub(self, y_cols):
        """
        Plot multiple time series subplots.

        Parameters:
        y_cols (list of str): The columns to plot.

        Returns:
        None

        Raises:
            KeyError: If any of the specified column names are not in the data.
        """
        num_plots = len(y_cols)
        fig, axs = plt.subplots(num_plots, 1, figsize=(8, num_plots * 4))
        x = self.data["Time"]
        for i, col in enumerate(y_cols):
            try:
                y = self.data[col]
            except KeyError:
                raise KeyError(f"Column '{col}' not found in data.")
            axs[i].plot(x, y, label=col)
            axs[i].set_xlabel("Time")
            axs[i].legend()
        plt.show()
    #
    # def plot_data_sub_scatter(self, y_cols):
    #     num_plots = len(y_cols)
    #     fig, axs = plt.subplots(num_plots, 1, figsize=(8, num_plots * 4))
    #     x = self.data["Time"]
    #     for i, col in enumerate(y_cols):
    #         y = self.data[col]
    #         axs[i].scatter(x, y, label=col, s=0.25)
    #         axs[i].set_xlabel("Time")
    #         axs[i].legend()
    #     plt.show()
