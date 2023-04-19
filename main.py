import pandas as pd

import tempdrift
import treemotion as tms
from treemotion import Messung

if __name__ == "__main__":
    print("Start")
    test_dir = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\data_tests"
    csv_file = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2023_Kronensicherung_Plesse\020_Daten\TMS\CSV_Messung_001_Plesse_export_2023-03-22_24h\2023-03-22 000000__DatasA000-0000-0007.csv"
    db_file = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\treemotion\data_tests\test.db"

    messung_1 = Messung.read_from_csv(source_path=csv_file, messung_id=1, feedback=True)
    messung_1.write_to_db(db_file, feedback=True)


    print("Ende")
