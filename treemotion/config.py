# treemotion/config.py

from pathlib import Path

class Configuration:
    """
    Eine Klasse zur Verwaltung der Konfigurationseinstellungen für das Treemotion-Paket.
    """

    def __init__(self):
        """
        Initialisiert die Konfigurationswerte mit den Standardwerten.
        """
        self.working_directory = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\working_directory_2"
        # Erstelle Directory und Parents falls erforderlich
        Path(self.working_directory).mkdir(exist_ok=True, parents=True)

        # Logging
        self.log_level = "debug"  # debug, info, warning, critical, error
        self.log_directory = "log"

        # Database
        self.template_db_name = "database_template_1_0_0_0.db"  # Look in the "treemotion" directory for template.db

        # Projekt

        # Messreihe

        # Messung

        # Data
        self.df_columns = ['Time', 'East-West-Inclination', 'North-South-Inclination',
                           'Absolute-Inclination', 'Inclination direction of the tree',
                           'Temperature', 'East-West-Inclination - drift compensated',
                           'North-South-Inclination - drift compensated',
                           'Absolute-Inclination - drift compensated',
                           'Inclination direction of the tree - drift compensated']
        self.data_version_default = "raw"
        self.data_version_copy_default = "copy"

        # WindMessreihe

        self.dwd_data_directory = "wind_data_dwd"
        # Weitere Konstanten hier

    def set_working_directory(self, directory: str):
        from .utilities.log import get_logger
        logger = get_logger(__name__)

        try:
            path = Path(directory)
            if not path.exists():
                path.mkdir(parents=True)  # Verzeichnis erstellen, falls es nicht existiert
                logger.info(f"Das Verzeichnis {directory} wurde erfolgreich erstellt.")
            else:
                logger.warning(f"Das Verzeichnis {directory} existiert bereits.")
                self.working_directory = str(path.resolve())  # Verwende das bestehende Verzeichnis

            self.working_directory = str(path.resolve())
            logger.info("Arbeitsverzeichnis festgelegt!")
            return True

        except Exception as e:
            logger.error(f"Fehler beim Festlegen des Arbeitsverzeichnisses: {str(e)}")
            return False


### README

# Um auf die Config-Werte aus einem beliebigen Modul zugreifen zu können, müssen Sie das configuration-Objekt aus dem config-Modul importieren. Hier ist ein Beispiel, wie Sie darauf zugreifen können:
#
# python
#
# from treemotion.config import configuration
#
# # Zugriff auf die Config-Werte
# log_level = configuration.log_level
# template_db_name = configuration.template_db_name
#
# # Verwendung der Config-Werte
# print(log_level)
# print(template_db_name)
#
# Auf diese Weise können Sie die Config-Werte in einem beliebigen Modul verwenden, indem Sie from treemotion.config import configuration importieren und dann auf die gewünschten Werte über configuration.attribut_name zugreifen.
#
# Wenn Sie die Config-Werte aus einem beliebigen Modul ändern möchten, können Sie dies direkt am configuration-Objekt tun. Hier ist ein Beispiel:
#
# python
#
# from treemotion.config import configuration
#
# # Ändern der Config-Werte
# configuration.log_level = "info"
# configuration.template_db_name = "new_template.db"
#
# Beachten Sie, dass diese Änderungen global sind und sich auf alle Teile Ihres Projekts auswirken, da das configuration-Objekt als Modulattribut definiert ist und von verschiedenen Modulen aus importiert wird.
