import pandas as pd
from typing import List


def validate_dataframe(df, columns: List = None):
    """
    Prüft, ob ein Objekt ein gültiger Pandas DataFrame ist.

    Parameters
    ----------
    df : object
        Das zu prüfende Objekt.
    columns : list of str
        Liste der erforderlichen Spaltennamen.

    Returns
    -------
    bool
        Gibt True zurück, wenn alle Überprüfungen bestehen.

    Raises
    ------
    TypeError
        Wenn das übergebene Objekt kein DataFrame ist.
    ValueError
        Wenn der DataFrame nicht die erforderlichen Spalten oder keine Zeilen hat,
        oder wenn er fehlende Werte enthält.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError('Das übergebene Objekt ist kein DataFrame.')

    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f'Im DataFrame fehlen die folgenden Spalten: {missing_cols}.')

    if df.empty:
        raise ValueError('Der DataFrame hat keine Zeilen.')

    return True


def _validate_inputs(df1: pd.DataFrame, df2: pd.DataFrame, column: str) -> None:
    """
    Validate the input dataframes and column names.

    Parameters:
    df1, df2: The two dataframes to be validated.
    column: The column to be validated.

    Raises:
    ValueError: If the dataframes or the column are invalid.
    """
    for df in (df1, df2):
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"Expected a DataFrame, but got {type(df).__name__}.")

    if not isinstance(column, str):
        raise ValueError(f"Expected a string for column, but got {type(column).__name__}.")
