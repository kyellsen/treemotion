# treemotion/classes/data.py
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
import pandas as pd
import matplotlib.pyplot as plt

from utilities.base import Base
from utilities.timing import timing_decorator
from utilities import tms_basics, tempdrift

from utilities.log import get_logger
logger = get_logger(__name__)


class Data(Base):
    __tablename__ = 'Data'
    id_data = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_messung = Column(Integer, ForeignKey('Messung.id_messung'))
    version = Column(String)
    table_name = Column(String)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    duration = Column(DateTime)
    length = Column(Integer)

    def __init__(self, id_data=None, id_messung=None, version=None, table_name=None, datetime_start=None,
                 datetime_end=None, duration=None, length=None, data=None):
        # in SQLite Database
        self.id_data = id_data
        self.id_messung = id_messung
        self.version = version
        self.table_name = table_name  # name of SQLite Table, where data is stored, f"auto_data_{version}_id_messung_{id_messung}"
        self.datetime_start = datetime_start  # metadata
        self.datetime_end = datetime_end  # metadata
        self.duration = duration  # metadata
        self.length = length  # metadata
        # additional only in class-object
        self.data = data

    @classmethod
    @timing_decorator
    def from_database(cls, db_data, session):
        obj = cls()
        obj.id_data = db_data.id_data
        obj.id_messung = db_data.id_messung
        obj.version = db_data.version
        obj.table_name = db_data.table_name
        obj.datetime_start = db_data.datetime_start
        obj.datetime_end = db_data.datetime_end
        obj.duration = db_data.duration
        obj.length = db_data.length
        obj.data = pd.read_sql_table(db_data.table_name, session.bind)
        return obj

    @timing_decorator
    def to_database(self, session):
        # Speichern der Metadaten in der Data-Tabelle
        if self.id_data is None:
            session.add(self)
            session.commit()
        else:
            session.merge(self)
            session.commit()

        # Speichern des DataFrames data in der neu erstellten Tabelle
        self.data.to_sql(self.table_name, session.bind, if_exists='replace', index=False)

    def update_metadata(self):
        # self.datetime_start = self.data['Time'].min()
        # self.datetime_end = self.data['Time'].max()
        # self.duration = self.datetime_end - self.datetime_start
        self.length = len(self.data)

    def limit_time(self, start_time, end_time):
        self.data = tms_basics.limit_by_time(self.data, time_col="Time", start_time=start_time, end_time=end_time)
        self.update_metadata()

    def random_sample(self, n):
        self.data = self.data.sample(n)
        self.update_metadata()

    @timing_decorator
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
        self.data[f'Absolute-Inclination - drift compensated{suffix}'] = tms_basics.get_absolute_inclination(x, y)
        self.data[
            f'Inclination direction of the tree - drift compensated{suffix}'] = tms_basics.get_inclination_direction(
            x, y)
        if feedback is True:
            print(
                f"Messung.temp_drift_comp - id_messung: {self.id_messung}")

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
