import pandas as pd
import pandera as pa
from pandera import DataFrameSchema, Check, Column
import plotly.graph_objects as go
import os
import logging
from typing import Union, List


class ValidationManager:
    """Eine Klasse zur Verwaltung der Datenvalidierung."""

    def __init__(self, schema: DataFrameSchema, save_dir: str):
        """
        Der Konstruktor für die ValidationManager-Klasse.

        Parameter:
        schema (DataFrameSchema): Das Pandera-Schema zur Validierung der Daten.
        save_dir (str): Das Verzeichnis, in dem die Validierungsberichte gespeichert werden.
        """
        self.schema = schema
        self.save_dir = save_dir

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validiert die Daten anhand des Schemas.

        Parameter:
        df (pd.DataFrame): Der DataFrame, der validiert werden soll.

        Rückgabe:
        df (pd.DataFrame): Der validierte DataFrame.
        """
        try:
            valid_df = self.schema.validate(df)
            logging.info('Datenvalidierung erfolgreich.')
            return valid_df
        except Exception as e:
            logging.error(f'Datenvalidierung fehlgeschlagen: {e}')
            raise e

    def visualize_results(self, df: pd.DataFrame) -> None:
        """
        Visualisiert die Ergebnisse der Datenvalidierung.

        Parameter:
        df (pd.DataFrame): Der DataFrame, der visualisiert werden soll.
        """
        try:
            for column in df.columns:
                fig = go.Figure(data=go.Scatter(x=df.index, y=df[column]))
                fig.update_layout(title_text=f'Datenverteilung für {column}')
                fig.show()
            logging.info('Visualisierung erfolgreich.')
        except Exception as e:
            logging.error(f'Visualisierung fehlgeschlagen: {e}')
            raise e

    def create_report(self, df: pd.DataFrame, file_name: str) -> None:
        """
        Erstellt einen Validierungsbericht und speichert ihn im festgelegten Verzeichnis.

        Parameter:
        df (pd.DataFrame): Der DataFrame, für den der Bericht erstellt werden soll.
        file_name (str): Der Dateiname des Berichts.
        """
        try:
            report = df.describe().transpose()
            report.to_csv(os.path.join(self.save_dir, f'{file_name}.csv'))
            logging.info('Berichtserstellung erfolgreich.')
        except Exception as e:
            logging.error(f'Berichtserstellung fehlgeschlagen: {e}')
            raise e


schema = DataFrameSchema({
    'Time': Column(pa.DateTime, Check.is_not_null()),
    'East-West-Inclination': Column(pa.Float, Check.is_not_null(), Check.in_range(-1, 1)),
    'North-South-Inclination': Column(pa.Float, Check.is_not_null(), Check.in_range(-1, 1)),
    'Absolute-Inclination': Column(pa.Float, Check.is_not_null(), Check.in_range(0, 1)),
    'Inclination direction of the tree': Column(pa.Float, Check.is_not_null(), Check.in_range(0, 360)),
    'Temperature': Column(pa.Float, Check.is_not_null(), Check.in_range(-50, 50)),
    'East-West-Inclination - drift compensated': Column(pa.Float, Check.is_not_null(), Check.in_range(-1, 1)),
    'North-South-Inclination - drift compensated': Column(pa.Float, Check.is_not_null(), Check.in_range(-1, 1)),
    'Absolute-Inclination - drift compensated': Column(pa.Float, Check.is_not_null(), Check.in_range(0, 1)),
    'Inclination direction of the tree - drift compensated': Column(pa.Float, Check.is_not_null(),
                                                                    Check.in_range(0, 360)),
})
