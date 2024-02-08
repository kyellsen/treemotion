import unittest
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from treemotion.tms.rotate import rotate_l_reg, rotate_pca, rotate_to_align_x_axis


class TestRotationFunction(unittest.TestCase):
    def rotate_data(self, x, y, angle_deg):
        """Hilfsfunktion, um den Datensatz zu drehen."""
        angle_rad = np.radians(angle_deg)
        cos_angle = np.cos(angle_rad)
        sin_angle = np.sin(angle_rad)
        x_rotated = x * cos_angle - y * sin_angle
        y_rotated = x * sin_angle + y * cos_angle
        return x_rotated, y_rotated

    def test_rotation_multiple_angles(self):
        """Testet die Rotationsfunktion für mehrere zufällige Winkel."""
        np.random.seed(42)  # Für reproduzierbare Ergebnisse
        angles = np.random.uniform(low=0, high=90, size=10)  # 10 zufällige Winkel zwischen 0 und 90 Grad

        s = 1000000  # Größe des simulierten Datensatzes
        # Erzeugen eines Datensatzes mit größerer Streuung in der x-Richtung
        x_original = np.random.normal(loc=0, scale=2, size=s)  # Größere Streuung in x-Richtung
        y_original = np.random.normal(loc=0, scale=1, size=s)  # Normale Streuung in y-Richtung

        for expected_angle_deg in angles:
            # Datensatz um den erwarteten Winkel drehen
            x_rotated, y_rotated = self.rotate_data(x_original, y_original, expected_angle_deg)
            x_series = pd.Series(x_rotated)
            y_series = pd.Series(y_rotated)

            # Rotationsfunktion aufrufen
            x_rotated_back, y_rotated_back = rotate_to_align_x_axis(x_series, y_series)

            # Linear Regression, um den tatsächlichen Drehwinkel zu überprüfen
            model = LinearRegression().fit(x_rotated_back.values.reshape(-1, 1), y_rotated_back.values)
            b = model.coef_[0]
            actual_angle_rad = np.arctan(b)
            actual_angle_deg = np.degrees(actual_angle_rad)

            # Berechnen des erwarteten Rückdrehwinkels
            expected_back_angle_deg = -expected_angle_deg

            # Überprüfen, ob der Drehwinkel innerhalb eines akzeptablen Fehlers liegt
            self.assertAlmostEqual(expected_back_angle_deg, actual_angle_deg, delta=1,
                                   msg=f"Test failed for rotation angle {expected_angle_deg} degrees. Expected {expected_back_angle_deg}, got {actual_angle_deg}")


if __name__ == '__main__':
    unittest.main()
