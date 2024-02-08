from typing import Tuple
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


def rotate_pca(x_data: pd.Series, y_data: pd.Series) -> (pd.Series, pd.Series):
    angle_rad, angle_deg = calc_angle_pca(x_data, y_data)

    x_rotated, y_rotated = rotate_data(x_data, y_data, angle_rad)

    return x_rotated, y_rotated, angle_rad, angle_deg


def calc_angle_pca(x_data, y_data, standardize=False) -> Tuple[float, float]:
    """
    Berechnet den Winkel der ersten Hauptkomponente gegen die x-Achse,
    optional nach Standardisierung der Daten.

    :param x_data: pd.Series mit x-Werten der Datenpunkte
    :param y_data: pd.Series mit y-Werten der Datenpunkte
    :param standardize: Bool, ob die Daten vor der PCA standardisiert werden sollen
    :return: Winkel in Grad
    """
    data = np.vstack([x_data, y_data]).T

    if standardize:
        scaler = StandardScaler()
        data = scaler.fit_transform(data)

    pca = PCA(n_components=2)
    pca.fit(data)

    first_component = pca.components_[0]

    angle_rad = np.arctan2(first_component[1], first_component[0])
    angle_deg = np.degrees(angle_rad)
    return angle_rad, angle_deg


def rotate_data(x_data: pd.Series, y_data: pd.Series, angle_rad: float) -> Tuple[pd.Series, pd.Series]:
    """
    Rotates the data by the given angle while preserving the DateTimeIndex.

    :param x_data: pd.Series with x-values of data points, including DateTimeIndex.
    :param y_data: pd.Series with y-values of data points, including DateTimeIndex.
    :param angle_rad: Angle in radians.
    :return: Rotated data as a tuple of pd.Series (x_rotated, y_rotated) with DateTimeIndex preserved.
    """
    # Ensure that x_data and y_data have the same index
    if not x_data.index.equals(y_data.index):
        raise ValueError("x_data and y_data must have the same index.")

    # Convert Series to numpy array for rotation
    data = np.vstack((x_data.values, y_data.values)).T
    rotation_matrix = np.array([[np.cos(angle_rad), -np.sin(angle_rad)],
                                [np.sin(angle_rad), np.cos(angle_rad)]])
    data_rotated = np.dot(data, rotation_matrix)

    # Convert rotated data back to pd.Series, preserving the original DateTimeIndex
    x_rotated = pd.Series(data_rotated[:, 0], index=x_data.index)
    y_rotated = pd.Series(data_rotated[:, 1], index=y_data.index)

    return x_rotated, y_rotated
