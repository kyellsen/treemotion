# treemotion/classes/data.py
from sqlalchemy import text
import pandas as pd
from datetime import datetime
# import matplotlib.pyplot as plt

from utilities.imports_classes import *
from utilities.path_utils import validate_and_get_filepath
from tms.time_utils import validate_time_format, limit_df_by_time

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

    def __init__(self, *args, id_data: int = None, id_messung: int = None, version: str = None, table_name: str = None,
                 datetime_start: datetime = None, datetime_end: datetime = None, duration: datetime = None,
                 length: int = None, df: pd.DataFrame = None, **kwargs):
        """
        Initialisiert eine Data-Instanz.

        :param id_data: Eindeutige ID der Daten.
        :param id_messung: ID der Messung, zu der die Daten gehören.
        :param version: Version der Daten.
        :param table_name: Name der SQLite-Tabelle, in der die Daten gespeichert sind.
        :param datetime_start: Startzeitpunkt der Daten.
        :param datetime_end: Endzeitpunkt der Daten.
        :param duration: Dauer der Daten.
        :param length: Länge der Daten.
        :param df: Datenrahmen mit den Daten.
        """
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
        return f"Data(id={self.id_data}, table_name={self.table_name})"

    @timing_decorator
    def load_data(self, session=None):
        """
        Lädt die Daten für diese Data-Instanz aus der Datenbank.

        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Das Datenobjekt mit geladenen Daten oder None, wenn ein Fehler aufgetreten ist.
        """
        session = db_manager.get_session(session)
        try:
            self.df = pd.read_sql_table(self.table_name, session.bind)
            logger.info(f"Data.df erfolgreich geladen: {self.__str__()}")
        except Exception as e:
            logger.error(f"Data.df konnte nicht geladen werden: {self.__str__()}, error: {e}")
            return None
        return self

    def load_data_if_needed(self, session=None):
        """
        Lädt die Daten für diese Data-Instanz, falls sie noch nicht geladen wurden.

        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: True, wenn das Laden erfolgreich war, sonst False.
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.warning(
                f"Für Ausgangsversion {self.__str__()} fehlt der DataFrame (Data.df). Es wird automatisch Data.load_data() ausgeführt.")
            try:
                self.load_data(session=session)
                return True
            except Exception as e:
                logger.critical(f"Data.load_data konnte für {self.__str__()} nicht ausgeführt werden, error: {e}")
                return False

    # Geerbt von BaseClass
    @classmethod
    @timing_decorator
    def load_from_db(cls, id_messung: int = None, load_related_df: bool = False, session=None):
        """
        Lädt Daten aus der Datenbank.

        :param id_messung: ID der Messung, zu der die Daten gehören.
        :param load_related_df: Ob der zugehörige Datenrahmen geladen werden soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Liste von Datenobjekten.
        """
        objs = super().load_from_db(filter_by={'id_messung': id_messung} if id_messung else None, session=session)
        if not objs:
            logger.error(f"Keine Daten gefunden für id_messung={id_messung}")
        else:
            logger.info(f"{len(objs)} Data-Objekte wurden erfolgreich geladen.")
        if load_related_df:
            for obj in objs:
                obj.load_data()
        return objs

    @classmethod
    @timing_decorator
    def load_from_csv(cls, filepath: str, id_data, id_messung: int, version: str, table_name: str):
        """
        Lädt Daten aus einer CSV-Datei.

        :param filepath: Pfad zur CSV-Datei.
        :param id_data: Eindeutige ID der Daten.
        :param id_messung: ID der Messung, zu der die Daten gehören.
        :param version: Version der Daten.
        :param table_name: Name der SQLite-Tabelle, in der die Daten gespeichert sind.
        :return: Datenobjekt.
        """
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

    @classmethod
    def create_new_version(cls, source_obj, version_new: str):
        """
        Erstellt eine neue Version des Datenobjekts.

        :param source_obj: Das Quellobjekt, das kopiert werden soll.
        :param version_new: Die neue Version.
        :return: Neues Datenobjekt.
        """
        new_obj = cls()
        try:
            new_obj.id_data = None
            new_obj.id_messung = source_obj.id_messung
            new_obj.version = version_new
            new_obj.table_name = new_obj.new_table_name(new_obj.version, new_obj.id_messung)
            new_obj.df = source_obj.df.copy(deep=True)
            new_obj.update_metadata()
            return new_obj
        except Exception as e:
            logger.error(f"Fehler beim Erstellen der Kopie der Dateninstanz: {e}")
            return None

    @staticmethod
    @timing_decorator
    def read_csv_tms(filepath: str):
        """
        Liest Daten aus einer CSV-Datei.

        :param filepath: Pfad zur CSV-Datei.
        :return: Datenrahmen mit den gelesenen Daten.
        """
        try:
            filepath = validate_and_get_filepath(filepath)
        except Exception as e:
            return None
        try:
            df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",", index_col=False)

        except pd.errors.ParserError:
            logger.error(f"Fehler beim Lesen der Datei {filepath.stem}. Überprüfen Sie das Dateiformat.")
            return None
        except Exception as e:
            logger.error(f"Ungewöhnlicher Fehler beim Laden der {filepath.stem}: {e}")
            return None

        return df

    @staticmethod
    def new_table_name(version: str, id_messung: int) -> str:
        """
        Erzeugt einen neuen Tabellennamen.

        :param version: Version der Daten.
        :param id_messung: ID der Messung, zu der die Daten gehören.
        :return: Neuer Tabellenname.
        """
        return f"auto_df_{version}_{str(id_messung).zfill(3)}_messung"

    def update_metadata(self):
        """
        Aktualisiert die Metadaten des Datenobjekts (Data.df).
        """
        try:
            # self.datetime_start = self.df['Time'].min()
            # self.datetime_end = self.df['Time'].max()
            # self.duration = self.datetime_end - self.datetime_start
            self.length = len(self.df)

            if self.length == 0:
                logger.warning(
                    f"Die Aktualisierung der Metadata von {self.__str__()} ergibt einen leeren DataFrame (self.length = 0).")
            # logger.debug(f"Metadaten für {self.__str__()} erfolgreich aktualisert!")
        except Exception as e:
            logger.error(f"Metadaten für {self.__str__()} konnten nicht aktualisiert werden: {e}")

    @timing_decorator
    def commit_data(self, session=None):
        """
        Fügt das Datenobjekt zur Datenbank hinzu und führt den Commit aus.

        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        """
        session = db_manager.get_session(session)
        try:
            session.add(self)
            if self.df is not None:
                self.df.to_sql(self.table_name, session.bind, if_exists='replace', index=False)
            db_manager.commit(session)
            logger.debug(f"Instance {self.__str__()} committed.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error committing {self.__str__()} to Database: {e}")

    # Geerbt von BaseClass
    def remove(self, session=None, **kwargs):
        """
        Entfernt das Datenobjekt aus der Datenbank.

        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        """
        session = db_manager.get_session(session)
        existing_obj = session.query(type(self)).get(getattr(self, 'id_data'))
        try:
            if existing_obj is not None:
                # Delete the table associated with this Data object
                drop_table_statement = text(f"DROP TABLE IF EXISTS {self.table_name}")
                session.execute(drop_table_statement)
                self.df = None
                session.delete(existing_obj)
                logger.info(f"Objekt {self.__class__.__name__} wurde entfernt.")
                db_manager.commit(session)
            else:
                logger.info(f"Objekt {self.__class__.__name__} ist nicht vorhanden.")
        except Exception as e:
            session.rollback()  # Rollback the changes on error
            logger.error(f"Fehler beim Entfernen des Objekts {self.__class__.__name__}: {e}")

    # Geerbt von BaseClass
    @timing_decorator
    def copy(self, reset_id: bool = False, auto_commit: bool = False, session=None):
        """
        Erstellt eine Kopie des Datenobjekts.

        :param reset_id: Ob die ID des neuen Objekts zurückgesetzt werden soll.
        :param auto_commit: Ob ein automatischer Commit nach dem Kopieren erfolgen soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Kopiertes Datenobjekt.
        """
        new_obj = super().copy('id_data', reset_id, auto_commit, session)

        # Create a deep copy of the DataFrame
        if self.df is not None:
            new_obj.df = self.df.copy(deep=True)

        return new_obj

    # Geerbt von BaseClass
    def limit_by_time(self, start_time: str, end_time: str, auto_commit: bool = False, session=None):
        """
        Begrenzt die Daten auf einen bestimmten Zeitraum.

        :param start_time: Startzeitpunkt der Begrenzung.
        :param end_time: Endzeitpunkt der Begrenzung.
        :param auto_commit: Ob ein automatischer Commit nach dem Begrenzen des Zeitraums erfolgen soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Selbstreferenz für Methodenverkettung, None bei Fehler.
        """
        # Überprüfung der Zeitangaben
        start_time = validate_time_format(start_time)
        end_time = validate_time_format(end_time)
        if start_time is None or end_time is None:
            logger.error(f"Das Zeitformat ist ungültig.")
            return self.df

        # Überprüfung des DataFrames
        if self.df is None or self.df.empty:
            logger.warning(f"Der DataFrame von {self.__str__()} ist None oder leer.")
            return self.df

        # Limitierung Zeit
        try:
            self.df = limit_df_by_time(self.df, time_col="Time", start_time=start_time, end_time=end_time)
        except Exception as e:
            logger.error(f"Fehler der Limitierung der Daten von '{self.__str__()}', error: {str(e)}")
            return self.df

        logger.debug(f"Limitierung der Daten von '{self.__str__()}' zwischen {start_time} und {end_time} erfolgreich.")
        self.update_metadata()

        if auto_commit:
            self.commit_data(session=session)

        return self

    def random_sample(self, n: int, auto_commit: bool = False, session=None):
        """
        Wählt eine zufällige Stichprobe von Daten aus.

        :param n: Anzahl der auszuwählenden Datenpunkte.
        :param auto_commit: Ob ein automatischer Commit nach der Auswahl erfolgen soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Selbstreferenz für Methodenverkettung.
        """
        logger.info(f"Zufällige Stichprobe von {n} Datenpunkten wird aus {self.__str__()} ausgewählt.")

        if n > len(self.df):
            logger.warning(
                f"Die N ({n}) ist größer als die Länge des DataFrames ({len(self.df)}). Es werden alle Datenpunkte ausgewählt.")
            n = len(self.df)

        try:
            self.df = self.df.sample(n)
            self.update_metadata()
            logger.debug(f"Zufällige Stichprobe von {n} Datenpunkten wurde ausgewählt: {self.__str__()}")
        except Exception as e:
            logger.error(f"Fehler beim Auswählen der zufälligen Stichprobe: {e}")
            return self

        if auto_commit:
            self.commit_data(session=session)

        return self

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
