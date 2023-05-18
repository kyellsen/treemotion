from datetime import datetime
import pandas as pd

def validate_time_format(time_str: str):
    """
    Überprüft und passt gegebenenfalls das Zeitformat an.

    :param time_str: Die zu validierende Zeit als Zeichenkette.
    :return: Die angepasste Zeit als Zeichenkette.
    """
    time_format_1 = "%Y-%m-%d %H:%M:%S.%f"
    time_format_2 = "%Y-%m-%d %H:%M:%S"

    try:
        datetime.strptime(time_str, time_format_1)
    except ValueError:
        try:
            datetime.strptime(time_str, time_format_2)
        except ValueError:
            return None
        else:
            time_str += '.000000'

    return time_str


def limit_df_by_time(data: pd.DataFrame, time_col: str, start_time, end_time) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame that is limited to the specified time range.

    Parameters:
    -----------
    data : pandas DataFrame
        The original data frame.

    time_col : str
        Name of the column containing the timestamp.

    start_time : str
        Start time in the format 'yyyy-mm-dd hh:mm:ss'.

    end_time : str
        End time in the format 'yyyy-mm-dd hh:mm:ss'.

    Returns:
    --------
    pandas DataFrame
        A new Pandas DataFrame that contains only rows that have a timestamp
        within the specified range.
    """
    data = data.loc[(data[time_col] >= start_time) & (data[time_col] <= end_time)]
    data.reset_index(drop=True, inplace=True)

    return data
