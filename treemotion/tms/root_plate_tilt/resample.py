def resample_df(df: pd.DataFrame, column: str = 'datetime', sample_rate: float, method: str = 'linear') -> pd.DataFrame:
    """
    Resample a pandas DataFrame based on a specified sample rate and interpolation method.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame to be resampled.

    column : str, default='datetime'
        The column name containing datetime objects.

    sample_rate : float
        The sample rate in Hertz.

    method : str, default='linear'
        The method of interpolation to use. Can be 'linear', 'nearest', 'zero', 'slinear', 'quadratic', 'cubic', 'barycentric', 'polynomial'.

    Returns
    -------
    pandas.DataFrame
        The resampled DataFrame.

    Raises
    ------
    ValueError
        If the `column` is not in the DataFrame columns or the `method` is not a valid interpolation method.
    """

    if column not in df.columns:
        raise ValueError(f"{column} is not a column in the DataFrame")

    if method not in ['linear', 'nearest', 'zero', 'slinear', 'quadratic', 'cubic', 'barycentric', 'polynomial']:
        raise ValueError(f"{method} is not a valid interpolation method")

    # Set the datetime column as the index
    df = df.set_index(column)

    # Convert sample rate in Hz to pandas frequency string
    frequency = str(int(1 / sample_rate)) + 'S'

    # Perform the resampling
    resampled_df = df.resample(frequency).interpolate(method=method)

    return resampled_df
