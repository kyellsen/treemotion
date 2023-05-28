from pathlib import Path
import pandas as pd
import great_expectations as ge
from great_expectations import dataset
from typing import Union, List
from treemotion import config
from utils.log import get_logger

logger = get_logger(__name__)


class ValidationManager:
    """Eine Klasse zur Verwaltung der Datenvalidierung."""

    def __init__(self, df_expectations: ge.dataset.PandasDataset = None, report_directory: str = None):
        """
        Der Konstruktor für die ValidationManager-Klasse.

        Parameter:
        df_expectations (PandasDataset): Die Great Expectations für den DataFrame.
        report_directory (str): Das Verzeichnis, in dem die Validierungsberichte gespeichert werden.
        """
        if df_expectations is None:
            self.df_expectations = config.validation_manager_expectations
        else:
            self.df_expectations = df_expectations
        if report_directory is None:
            self.report_directory = config.validation_manager_directory
        else:
            self.report_directory = report_directory

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validiert die Daten anhand der Erwartungen.

        Parameter:
        df (pd.DataFrame): Der DataFrame, der validiert werden soll.

        Rückgabe:
        df (pd.DataFrame): Der validierte DataFrame.
        """
        ge_df = ge.from_pandas(df)
        result = ge_df.validate(expectation_suite=self.df_expectations, only_return_failures=True)
        if len(result["results"]) > 0:
            logger.error('Datenvalidierung fehlgeschlagen.')
            return result
        else:
            logger.info('Datenvalidierung erfolgreich.')
        return df

    # def visualize_results(self, df: pd.DataFrame) -> None:
    #     """
    #     Visualisiert die Ergebnisse der Datenvalidierung.
    #
    #     Parameter:
    #     df (pd.DataFrame): Der DataFrame, der visualisiert werden soll.
    #     """
    #     try:
    #         for column in df.columns:
    #             fig = go.Figure(data=go.Scatter(x=df.index, y=df[column]))
    #             fig.update_layout(title_text=f'Datenverteilung für {column}')
    #             fig.show()
    #         logging.info('Visualisierung erfolgreich.')
    #     except Exception as e:
    #         logging.error(f'Visualisierung fehlgeschlagen: {e}')
    #         raise e

    def create_report(self, df: pd.DataFrame, file_name: str) -> None:
        """
        Erstellt einen Validierungsbericht und speichert ihn im festgelegten Verzeichnis.

        Parameter:
        df (pd.DataFrame): Der DataFrame, für den der Bericht erstellt werden soll.
        file_name (str): Der Dateiname des Berichts.
        """
