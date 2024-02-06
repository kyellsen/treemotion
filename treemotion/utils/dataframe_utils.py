import pandas as pd
from typing import List, Optional


def validate_df(df: pd.DataFrame, columns: Optional[List[str]] = None) -> bool:
    """
    Validate if an object is a valid pandas DataFrame and check for the existence of specific columns.
    If the column list is None, it won't check for columns. Other columns not in the list are permissible.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame object to validate.
    columns : List[str], optional
        List of required column names.

    Returns
    -------
    bool
        Returns True if all validations are passed.

    Raises
    ------
    TypeError
        If the input object is not a DataFrame.
    ValueError
        If the DataFrame doesn't contain the required columns, has no rows,
        or contains missing values.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a DataFrame, but got {type(df).__name__}.")

    if df.empty:
        raise ValueError("The DataFrame has no rows.")

    if columns:
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"The DataFrame is missing the following columns: {missing_cols}.")

    return True
