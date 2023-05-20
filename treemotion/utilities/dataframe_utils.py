import pandas as pd


def validate_dataframe(df, columns):
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
