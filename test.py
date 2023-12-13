import treemotion as tms
from pathlib import Path


from treemotion.classes import Measurement, Series, Project


# Beispiel für die Verwendung
if __name__ == "__main__":
    # Main
    main_path = Path(r"C:\kyellsen\005_Projekte\2022_Bosau")
    analyse_name = r"2022_Bosau_2023-12-11"
    data_path = main_path / "021_Daten_Clean"  # Für alle Daten-Importe des Projektes gemeinsam
    working_directory = main_path / "030_Analysen" / analyse_name / "working_directory"  # Für alle Daten-Exporte des Projektes gemeinsam
    db_name = "TREEMOTION_Bosau_2023-05-12.db"
    source_db = data_path / db_name

    tcc_working_directory = working_directory / 'TMS'
    CONFIG, DATA_MANAGER, DATABASE_MANAGER, PLOT_MANAGER = tms.setup(working_directory=str(tcc_working_directory), log_level="DEBUG")
    DATABASE_MANAGER.duplicate(database_path=str(source_db))
    DATABASE_MANAGER.connect(db_name=str(db_name))
    session = DATABASE_MANAGER.get_session()

    #project = Project(project_name="Bosau_Test")
    #session.add(project)
    series_ls = session.query(Series).all()

    DATABASE_MANAGER.commit()
    #DATABASE_MANAGER.disconnect()
