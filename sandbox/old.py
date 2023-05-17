from sqlalchemy import Integer, String, types
import configparser
import pandas as pd
import os
from pathlib import Path
import dbi


def get_tms_filenames_from_db(data_path: str, db_path: str) -> pd.DataFrame:
    """
    Reads the "Messreihe" and "Messung" tables from the SQLite database specified by db_path and constructs a new DataFrame
    containing information about TMS files.

    :param data_path: A string representing the path to the directory containing the TMS files.
    :param db_path: A string representing the name of the SQLite database.
    :return: A pandas DataFrame containing information about the TMS files.
    """

    # Read the "Messreihe" table from the SQLite database and drop any rows with missing values.
    messreihe_filepath_tms_df = dbi.read_df(db_path, "messreihe")[["id_messreihe", "filepath_tms"]].dropna()

    # Initialize an empty DataFrame to hold information about the TMS files.
    file_df = pd.DataFrame()

    # Loop over the filepaths in the "Messreihe" table.
    for filepath in messreihe_filepath_tms_df["filepath_tms"]:
        # Get a list of filenames for the files in the current filepath.
        files = os.listdir(Path(data_path + filepath))

        # Create a list of filepaths for the files in the current filepath.
        paths = [filepath] * len(files)

        # Create a list of GIDs for the files in the current filepath.
        gids = [int(filename[-7:-4]) for filename in files]

        # Create a DataFrame with information about the files in the current filepath and add it to the overall DataFrame.
        file_df_i = pd.DataFrame(zip(gids, paths, files), columns=["gid", "filepath_tms", "filename"])
        file_df = pd.concat([file_df, file_df_i])

    file_df = file_df.merge(messreihe_filepath_tms_df, on='filepath_tms', how='left')

    # Merge the TMS file information with the "Messreihe" and "Messung" tables based on "id_messreihe" and "gid".
    df = dbi.read_df(db_path, "messung.py").merge(
        file_df, on=['id_messreihe', 'gid'], how='left')

    return df


def write_tms_filenames_to_db(data_path, db_path):
    # Get the TMS filenames DataFrame.
    tms_file_df = get_tms_filenames_from_db(data_path, db_path)
    # Define the data types for the columns in the target table.
    dtype_dict = {
        "id_messung": Integer,
        "id_messreihe": Integer,
        "id_baumbehandlung": Integer,
        "gid": Integer,
        "geraet_ort": String,
        "geraet_hoehe": Integer,
        "geraet_umfang": Integer,
        "geraet_ausrichtung": String,
        "status_daten": String,
        "filepath_tms": String,
        "filename": String
    }

    # Write the TMS filenames DataFrame to the target table in the SQLite database.
    dbi.write_df(db_path=db_path, df_name="auto_messung", data=tms_file_df, dtype_dic=dtype_dict)

def import_tms_to_db(data_path: str, db_path: str):
    # Read the list of CSV files to import from the "auto_messung" table of the existing database.
    auto_messung_df = dbi.read_df(db_path, "auto_messung")

    # Filter out rows with a null filename.
    auto_messung_df = auto_messung_df[auto_messung_df["filename"].notnull()]

    # Read the data types for the columns of the new table from a config file.
    config = configparser.ConfigParser()
    config.read('..\\config.txt')
    dtype_dic = {}
    for key, value in config.items('data_types'):
        dtype_dic[key] = getattr(types, value)

    # Loop over the rows of the "auto_messung" table.
    for i, row in auto_messung_df.iterrows():

        # Construct the full path to the current CSV file.
        filepath = Path(data_path + row["filepath_tms"] + "/" + row["filename"])
        print(row["filename"])

        # Read the CSV file into a pandas DataFrame.
        csv_df = pd.read_csv(filepath, delimiter=";", parse_dates=["Time"], decimal=",")

        # Add the "id_messung" column to the DataFrame.
        csv_df["id_messung"] = row["id_messung"]

        dbi.write_df(db_path, df_name="imported_data", data=csv_df, dtype_dic=dtype_dic, if_exists='append')



if __name__ == '__main__':
    tms_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Bachelorarbeit\020_Testdaten"
    db_path = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Bachelorarbeit\020_Testdaten\Bosau.db"
    print(tms_path)
    print(db_path)
    write_tms_filenames_to_db(data_path=tms_path, db_path=db_path)
    #import_tms_to_db(tms_path, db_path)
    print("ENDE MAIN")
    #df = dbi.read_sql(db_path=db_path, sql_query="SELECT * FROM messung.py")
    df = dbi.read_df(db_path=db_path, df_name="messung.py")
    print("HIER")


