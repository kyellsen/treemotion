# treemotion/classes/data.py
# from sqlalchemy.schema import DropTable, MetaData
import pandas as pd
# import matplotlib.pyplot as plt

from utilities.imports_classes import *
from utilities.path_utils import validate_and_get_filepath

logger = get_logger(__name__)


class Data(BaseClass):
    __tablename__ = 'Data'
    id_data = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_messung = Column(Integer, ForeignKey('Messung.id_messung', onupdate='CASCADE'), nullable=False)
    version = Column(String)
    table_name = Column(String)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    duration = Column(DateTime)
    length = Column(Integer)

    def __init__(self, *args, id_data=None, id_messung=None, version=None, table_name=None, datetime_start=None,
                 datetime_end=None, duration=None, length=None, df=None, **kwargs):
        super().__init__(*args, **kwargs)
        # in SQLite Database
        self.id_data = id_data
        self.id_messung = id_messung
        self.version = version
        self.table_name = table_name  # name of SQLite Table, where data is stored
        self.datetime_start = datetime_start  # metadata
        self.datetime_end = datetime_end  # metadata
        self.duration = duration  # metadata
        self.length = length  # metadata
        # additional only in class-object, own table in database
        self.df = df

    def __str__(self):
        return f"Data(id={self.id_data}, id_messung={self.id_messung}, table_name={self.table_name})"

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_messung=None, load_related_df=False, session=None):
        objs = super().load_from_db(filter_by={'id_messung': id_messung} if id_messung else None, session=session)
        logger.info(f"{len(objs)} Data-Objekte wurden erfolgreich geladen.")
        if load_related_df:
            for obj in objs:
                obj.load_df()
                logger.info(f"Data.df erfolgreiche geladen: {obj.__str__()}")
        return objs

    @staticmethod
    def read_csv_tms(filepath):
        try:
            filepath = validate_and_get_filepath(filepath)
        except:
            return None
        try:
            df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",")

        except pd.errors.ParserError:
            logger.error(f"Fehler beim Lesen der Datei {filepath.stem}. Überprüfen Sie das Dateiformat.")
            return None
        except Exception as e:
            logger.error(f"Ungewöhnlicher Fehler beim Laden der {filepath.stem}: {e}")
            return None

        return df

    def update_metadata(self):
        try:
            # self.datetime_start = self.df['Time'].min()
            # self.datetime_end = self.df['Time'].max()
            # self.duration = self.datetime_end - self.datetime_start
            self.length = len(self.df)
            logger.debug(f"Metadaten für {self.__str__()} erfolgreich aktualisert!")
        except Exception as e:
            logger.error(f"Metadaten für {self.__str__()} konnten nicht aktualisiert werden: {e}")


    @classmethod
    @timing_decorator
    def load_from_csv(cls, filepath, id_data, id_messung, version, table_name):
        if filepath is None:
            logger.warning(f"Filepath = None, Prozess abgebrochen.")
            return None
        obj = cls()
        obj.id_data = id_data
        obj.id_messung = id_messung
        obj.version = version
        obj.table_name = table_name
        obj.df = obj.read_csv_tms(filepath)
        obj.update_metadata()
        return obj


    @timing_decorator
    def load_df(self, session=None):
        self.df = pd.read_sql_table(self.table_name, session)
        return self

    def remove(self, id_name='id_data', auto_commit=False, session=None):
        session = db_manager.get_session(session)
        existing_obj = session.query(type(self)).get(getattr(self, id_name))
        try:
            if existing_obj is not None:
                # Delete the table associated with this Data object
                session.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                self.df = None
                session.delete(existing_obj)
                logger.info(f"Objekt {self.__class__.__name__} wurde entfernt.")
            else:
                logger.info(f"Objekt {self.__class__.__name__} ist nicht vorhanden.")
            if auto_commit:
                db_manager.commit(session)
        except Exception as e:
            session.rollback()  # Rollback the changes on error
            logger.error(f"Fehler beim Entfernen des Objekts {self.__class__.__name__}: {e}")

    @timing_decorator
    def copy(self, id_name="id_data", reset_id=False, auto_commit=False, session=None):
        new_instance = super().copy(id_name, reset_id, auto_commit, session)

        # Create a deep copy of the DataFrame
        if self.df is not None:
            new_instance.df = self.df.copy(deep=True)

        return new_instance

    def copy_deep(self, copy_relationships=True):
        copy = super().copy_deep(copy_relationships=copy_relationships)
        return copy

    @staticmethod
    def new_table_name(version: str, id_messung: int):
        return f"auto_df_{version}_{id_messung}_messung"



    #
    # def limit_time(self, start_time, end_time):
    #     self.df = tms_basics.limit_by_time(self.df, time_col="Time", start_time=start_time, end_time=end_time)
    #     self.update_metadata()
    #
    # def random_sample(self, n):
    #     self.df = self.df.sample(n)
    #     self.update_metadata()
    #
    # @timing_decorator
    # def temp_drift_comp(self, method="emd_hht", overwrite=True, sample_rate=20, window_size=1000,
    #                     freq_range=(0.05, 2, 128), feedback=False):  # 128 is used because ...
    #     """
    #     Compensate the temperature drift in the measurements using the specified method.
    #
    #     :param method: The method to use for temperature drift compensation.
    #     :param overwrite: Whether to overwrite the original data or create new columns.
    #     :param sample_rate: The sample rate of the data (in Hz).
    #     :param window_size: The window size for the moving average method.
    #     :param freq_range: The frequency range for the EMD-HHT method.
    #     :param feedback: Show result and runtime
    #     """
    #     temp = self.df['Temperature']
    #
    #     methods = {
    #         "lin_reg": lambda df: tempdrift.temp_drift_comp_lin_reg(df, temp),
    #         "lin_reg_2": lambda df: tempdrift.temp_drift_comp_lin_reg_2(df, temp),
    #         "mov_avg": lambda df: tempdrift.temp_drift_comp_mov_avg(df, window_size),
    #         "emd_hht": lambda df: tempdrift.temp_drift_comp_emd(df, sample_rate, freq_range),
    #     }
    #
    #     if method in methods:
    #         x = methods[method](self.df['East-West-Inclination'])
    #         y = methods[method](self.df['North-South-Inclination'])
    #     else:
    #         raise ValueError(f"Invalid method for temp_drift_comp: {method}")
    #
    #     suffix = "" if overwrite else " - new"
    #
    #     self.df[f'East-West-Inclination - drift compensated{suffix}'] = x
    #     self.df[f'North-South-Inclination - drift compensated{suffix}'] = y
    #     self.df[f'Absolute-Inclination - drift compensated{suffix}'] = tms_basics.get_absolute_inclination(x, y)
    #     self.df[
    #         f'Inclination direction of the tree - drift compensated{suffix}'] = tms_basics.get_inclination_direction(
    #         x, y)
    #     if feedback is True:
    #         print(
    #             f"Messung.temp_drift_comp - id_messung: {self.id_messung}")
    #
    # def plot_df(self, y_cols):
    #     """
    #     Plots the specified columns of df against time.
    #
    #     Args:
    #         y_cols (list of str): The names of the columns to plot on the y-axis.
    #
    #     Returns:
    #         None.
    #
    #     Raises:
    #         KeyError: If any of the specified column names are not in the df.
    #     """
    #     fig, ax = plt.subplots()
    #     x = self.df["Time"]
    #     for col in y_cols:
    #         try:
    #             y = self.df[col]
    #         except KeyError:
    #             raise KeyError(f"Column '{col}' not found in df.")
    #         ax.plot(x, y, label=col)
    #     ax.set_xlabel("Time")
    #     ax.legend()
    #     plt.show()
    #
    # def plot_df_sub(self, y_cols):
    #     """
    #     Plot multiple time series subplots.
    #
    #     Parameters:
    #     y_cols (list of str): The columns to plot.
    #
    #     Returns:
    #     None
    #
    #     Raises:
    #         KeyError: If any of the specified column names are not in the df.
    #     """
    #     num_plots = len(y_cols)
    #     fig, axs = plt.subplots(num_plots, 1, figsize=(8, num_plots * 4))
    #     x = self.df["Time"]
    #     for i, col in enumerate(y_cols):
    #         try:
    #             y = self.df[col]
    #         except KeyError:
    #             raise KeyError(f"Column '{col}' not found in df.")
    #         axs[i].plot(x, y, label=col)
    #         axs[i].set_xlabel("Time")
    #         axs[i].legend()
    #     plt.show()
