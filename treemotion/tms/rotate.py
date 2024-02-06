import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression
from pandas import Series


def rotate_pca(east_west_series: Series, north_south_series: Series) -> (Series, Series):
    """
    Rotates tree inclination data based on Principal Component Analysis (PCA) to align with the main wind direction.

    This approach follows Jackson et al. (2020), which uses PCA to identify the primary direction of tree sway,
    aligning data along the 'stream-wise' and 'cross-stream' components to represent the wind's dominant direction.

    Parameters:
    - east_west_series (pd.Series): Series containing East-West inclination data.
    - north_south_series (pd.Series): Series containing North-South inclination data.

    Returns:
    - Tuple of pd.Series: Rotated East-West and North-South inclination data.
    """
    # Combining inclination data into a NumPy array for PCA
    X = np.column_stack((east_west_series, north_south_series))

    # Applying PCA to find the main direction of tree sway
    pca = PCA(n_components=2)
    X_r = pca.fit_transform(X)

    # Returning the rotated series
    return pd.Series(X_r[:, 0]), pd.Series(X_r[:, 1])


def rotate_l_reg(east_west_series: Series, north_south_series: Series) -> (Series, Series):
    """
    Rotates tree inclination data based on linear regression to align with the main wind direction.

    Following Hoffmann et al. (2015), this method uses linear regression to determine the relationship
    between East-West and North-South inclination, then rotates the data accordingly to align with the wind direction.
    The rotation angle is derived from the regression coefficient, assuming larger inclination values occur along the wind direction.

    Parameters:
    - east_west_series (pd.Series): Series containing East-West inclination data.
    - north_south_series (pd.Series): Series containing North-South inclination data.

    Returns:
    - Tuple of pd.Series: Rotated East-West and North-South inclination data.
    """
    # Preparing data for linear regression
    X = east_west_series.values.reshape(-1, 1)
    y = north_south_series.values

    # Performing linear regression
    model = LinearRegression()
    model.fit(X, y)

    # Calculating the rotation angle from the regression coefficient
    b = model.coef_[0]
    alpha = np.arctan(b)

    # Performing the rotation
    cos_alpha, sin_alpha = np.cos(alpha), np.sin(alpha)
    x_rotated = east_west_series * cos_alpha + north_south_series * sin_alpha
    y_rotated = -east_west_series * sin_alpha + north_south_series * cos_alpha

    # Returning the rotated series
    return pd.Series(x_rotated), pd.Series(y_rotated)
