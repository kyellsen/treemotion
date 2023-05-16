# treemotion/classes/data.py
# from sqlalchemy.schema import DropTable, MetaData
import pandas as pd
# import matplotlib.pyplot as plt

from utilities.imports_classes import *


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
        return f"Data(id_data={self.id_data}, id_messung={self.id_messung}, version={self.version}, table_name={self.table_name})"

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_messung=None, load_related_df=False):
        objs = super().load_from_db(filter_by={'id_messung': id_messung} if id_messung else None)
        logger.info(f"{len(objs)} Data-Objekte wurden erfolgreich geladen.")
        if load_related_df:
            for obj in objs:
                obj.load_df()
                logger.info(f"Data.df erfolgreiche geladen: {obj.__str__()}")
        return objs

    @timing_decorator
    def load_df(self):
        self.df = pd.read_sql_table(self.table_name, db_manager.get_session().bind)
        return self

    @timing_decorator
    def commit_to_db(self, refresh=True):
        session = db_manager.get_session()
        session.merge(self)
        if refresh:
            session.refresh(self)
        db_manager.close_session()
        logger.info(
            f"Änderungen am {self.__class__.__name__} wurden erfolgreich in der Datenbank committet.")


    def commit_to_db(self, refresh=True):
        if self.df is not None:
            self.df.to_sql(self.table_name, session.bind, if_exists='replace', chunksize=20000)  ### PROOOFFF
        if refresh:
            session.refresh(self)
        logger.info(
            f"Änderungen am {self.__class__.__name__} wurden erfolgreich in der Datenbank committet.")


    @timing_decorator
    def remove_from_db(self):
        try:
            with db_manager.get_session_scope() as session:
                # Start a transaction
                session.begin()

                if self.table_name:
                    # Delete the table associated with this Data object
                    session.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                    logger.info(f"Tabelle {self.table_name} wurde aus der Datenbank gelöscht.")

                # Call the base class method to remove this Data object from the database
                super().remove_from_db(id_name='id_data')

        except SQLAlchemyError as e:
            logger.error(f"Fehler beim Entfernen des Data-Objekts {self.__str__()} aus der Datenbank: {e}")

    def copy(self, copy_relationships=False):
        copy = super().copy(copy_relationships=copy_relationships)
        return copy

    @staticmethod
    def new_table_name(version: str, id_messung: int):
        return f"auto_df_{version}_{id_messung}_messung"

    def update_metadata(self):
        # self.datetime_start = self.df['Time'].min()
        # self.datetime_end = self.df['Time'].max()
        # self.duration = self.datetime_end - self.datetime_start
        self.length = len(self.df)

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
