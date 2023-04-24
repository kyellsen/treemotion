import pandas as pd
import numpy as np

# from treemotion.utilities.basics import limit_data_by_time
# from treemotion.utilities.basics import random_sample
# from treemotion.utilities.basics import get_absolute_inclination
from treemotion.utilities.tms_basics import get_inclination_direction

def test_get_inclination_direction():
    # Test with positive x and y values
    x = pd.Series([1, 2, 3])
    y = pd.Series([1, 2, 3])
    expected_result = pd.Series([45.0, 45.0, 45.0])
    assert np.allclose(get_inclination_direction(x, y), expected_result)

    # Test with negative x and y values
    x = pd.Series([-1, -2, -3])
    y = pd.Series([-1, -2, -3])
    expected_result = pd.Series([225.0, 225.0, 225.0])
    assert np.allclose(get_inclination_direction(x, y), expected_result)

    # Test with mixed positive and negative values for x and y
    x = pd.Series([-1, 1, -1, 1])
    y = pd.Series([1, -1, -1, 1])
    expected_result = pd.Series([135.0, 315.0, 225.0, 45.0])
    assert np.allclose(get_inclination_direction(x, y), expected_result)

    # Test with x = 0 and positive y values
    x = pd.Series([0, 0, 0])
    y = pd.Series([1, 2, 3])
    expected_result = pd.Series([90.0, 90.0, 90.0])
    assert np.allclose(get_inclination_direction(x, y), expected_result)

    # Test with x = 0 and negative y values
    x = pd.Series([0, 0, 0])
    y = pd.Series([-1, -2, -3])
    expected_result = pd.Series([270.0, 270.0, 270.0])
    assert np.allclose(get_inclination_direction(x, y), expected_result)

    # Test with y = 0 and positive x values
    x = pd.Series([1, 2, 3])
    y = pd.Series([0, 0, 0])
    expected_result = pd.Series([0.0, 0.0, 0.0])
    assert np.allclose(get_inclination_direction(x, y), expected_result)

    # Test with y = 0 and negative x values
    x = pd.Series([-1, -2, -3])
    y = pd.Series([0, 0, 0])
    expected_result = pd.Series([180.0, 180.0, 180.0])
    assert np.allclose(get_inclination_direction(x, y), expected_result)
