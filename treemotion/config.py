# treemotion/config.py

class Configuration:
    """
    Eine Klasse zur Verwaltung der Konfigurationseinstellungen für das Treemotion-Paket.
    """

    def __init__(self):
        """
        Initialisiert die Konfigurationswerte mit den Standardwerten.
        """
        self.log_level = "info"  # debug, info, warning, critical, error
        self.log_directory = "log"
        self.template_db_name = "database_template_1_0_0_0.db"  # Look in the "treemotion" directory for template.db
        self.default_path_db = None  # Default database path

        # Projekt

        # Messreihe

        # Messung

        # Data
        self.data_version_default = "raw"
        # Weitere Konstanten hier


# Eine Instanz der Configuration-Klasse erstellen
configuration = Configuration()


def get_default_path_db(path_db):
    """
    Gibt den Standardpfad zur Datenbank zurück, falls kein spezifischer Pfad angegeben ist.

    Args:
        path_db (str): Der spezifische Pfad zur Datenbank.

    Returns:
        str: Der Pfad zur Datenbank.
    """
    if path_db is None:
        return configuration.default_path_db
    return path_db

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
