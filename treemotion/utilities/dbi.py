from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import pandas as pd


def test_connection(db_name):
    engine = create_engine('sqlite:///' + str(db_name))
    connection = engine.connect()
    if connection.closed == 0:
        print("Connection to Database successful")
        connection.close()
    else:
        print("Connection Error to Database")


def read_df(db_path, table_name):
    """
    Reads a SQLite database table into a pandas DataFrame.

    :param db_path: the name of the SQLite database to read from.
    :param table_name: the name of the database table to read.

    :return: a pandas DataFrame with the data from the specified database table.
    """

    # Create an engine to connect to the SQLite database with a 100-second timeout and a NullPool connection pool.
    engine = create_engine('sqlite:///' + str(db_path), connect_args={'timeout': 100}, poolclass=NullPool)

    # Return the DataFrame with the data from the specified database table.
    return pd.read_sql_table(table_name, con=engine)


def read_sql(db_path: str, sql_query: str):
    """
    Reads a SQLite database table into a pandas DataFrame using a specified SQL query.

    :param db_path: the path to the SQLite database to read from.
    :param sql_query: the SQL query to execute on the database.

    :return: a pandas DataFrame with the data from the specified database table.
    """

    # Create an engine to connect to the SQLite database with a 100-second timeout and a NullPool connection pool.
    engine = create_engine('sqlite:///' + str(db_path), connect_args={'timeout': 100}, poolclass=NullPool)

    # Read the data from the database using the specified SQL query into a pandas DataFrame.
    return pd.read_sql(sql=sql_query, con=engine)


def write_df(db_path, table_name, data_df, dtype_dict, if_exists='replace'):
    """
    Writes a pandas DataFrame to a SQLite database table using a specified data type dictionary and other options.

    :param if_exists:
    :param db_path: the name of the SQLite database to write to.
    :param table_name: the name of the database table to write to.
    :param data_df: the pandas DataFrame to write to the database table.
    :param dtype_dict: a dictionary of column names and their corresponding data types for the database table.

    :return: None.
    """

    # Create an engine to connect to the SQLite database
    engine = create_engine('sqlite:///' + str(db_path))

    # Write the data from the DataFrame to the database table using the specified data type dictionary and other options.
    data_df.to_sql(
        table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        dtype=dtype_dict
    )
