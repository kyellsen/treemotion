import treemotion as tms
from pathlib import Path

from treemotion.classes import Measurement, Series, Project, MeasurementVersion, DataTMS

# Beispiel für die Verwendung
if __name__ == "__main__":
    # Main
    main_path = Path(r"C:\kyellsen\005_Projekte\2022_Bosau")
    analyse_name = r"2022_Bosau_2023-12-11"
    data_path = main_path / "021_Daten_Clean"  # Für alle Daten-Importe des Projektes gemeinsam
    working_directory = main_path / "030_Analysen" / analyse_name / "working_directory"  # Für alle Daten-Exporte des Projektes gemeinsam
    db_name = "TREEMOTION_Bosau_2023-05-12.db"
    source_db = data_path / db_name
    csv_path = r"C:\kyellsen\005_Projekte\2022_Bosau\020_Daten"

    tcc_working_directory = working_directory / 'TMS'
    CONFIG, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER = tms.setup(working_directory=str(tcc_working_directory),
                                                                     log_level="DEBUG")
    DATABASE_MANAGER.duplicate(database_path=str(source_db))
    DATABASE_MANAGER.connect(db_name=str(db_name))
    project = DATABASE_MANAGER.load(Project, 1)[0]
    project.method_for_all_children("add_filenames", csv_path=csv_path)
    series = DATABASE_MANAGER.load(Series, 1)[0]
    measurement = DATABASE_MANAGER.load(Measurement, 1)[0]

    measurement.method_for_all_of_class("Measurement", "load_from_csv", overwrite=True)

    DataTMS_list = DATABASE_MANAGER.load(DataTMS)
    MeasurementVersion_list = DATABASE_MANAGER.load(MeasurementVersion)

    dfs = [DataTMS.data for DataTMS in DataTMS_list]

    print("Programm angehalten. Drücken Sie Enter, um fortzufahren...")
    input()
    print("Programm fortgesetzt.")

    session = DATABASE_MANAGER.session

    # Lösche jede Instanz einzeln
    for instance in MeasurementVersion_list:
        session.delete(instance)

    # Commit die Änderungen
    DATABASE_MANAGER.commit()

    # DATABASE_MANAGER.disconnect()
