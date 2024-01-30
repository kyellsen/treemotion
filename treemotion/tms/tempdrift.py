import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from scipy.signal import butter, filtfilt

SAMPLE_RATE = 20
LOW_FREQ_CUTOFF = 0.05
HIGH_FREQ_CUTOFF = 3.0


def temp_drift_comp_lin_reg(inclino: pd.Series, temperature: pd.Series) -> pd.Series:
    """
    Corrects inclination values based on linear regression, using centered temperature values.
    The centering of inclination data is done after compensating for temperature drift.

    Parameters:
    data (pd.Series): Series of inclination values.
    temperature (pd.Series): Series of temperature values.

    Returns:
    pd.Series: Corrected and centered inclination values.
    """
    # Centering the temperature around its median
    temperature_centered = temperature - temperature.median()

    # Creating a DataFrame for centered temperature data and inclination values
    df = pd.DataFrame({
        'temperature_centered': temperature_centered,
        'inclino': inclino
    })

    # Performing linear regression
    model = LinearRegression()
    model.fit(df[['temperature_centered']], df['inclino'])

    # Calculating the corrected inclination values
    predicted = model.predict(df[['temperature_centered']])
    corrected_inclino = df['inclino'] - predicted

    # Centering the corrected inclination values around their median
    corrected_inclino -= corrected_inclino.median()

    return corrected_inclino


def temp_drift_comp_lin_reg_2(inclino: pd.Series, temperature: pd.Series) -> pd.Series:
    """
    Corrects inclination values using linear regression with numpy's polyfit,
    based on centered temperature values. Centering of inclination data is done after
    compensating for temperature drift.

    Parameters:
    data (pd.Series): Series of inclination values.
    temperature (pd.Series): Series of temperature values.

    Returns:
    pd.Series: Corrected and centered inclination values.
    """
    # Centering the temperature around its median
    temperature_centered = temperature - temperature.median()

    # Linear regression for inclination in relation to temperature
    coeff_1, coeff_2 = np.polyfit(temperature_centered, inclino, deg=1)

    # Calculation of the trend line
    trendline = temperature_centered * coeff_1 + coeff_2

    corrected_inclino = inclino - trendline

    # Centering the corrected inclination values around their median
    corrected_inclino -= corrected_inclino.median()

    return corrected_inclino


def temp_drift_comp_mov_avg(inclino: pd.Series, window_size: int = 1001) -> pd.Series:
    # Gleitenden Durchschnitt berechnen
    rolling_mean = inclino.rolling(window=window_size).mean()
    inclino = inclino - rolling_mean

    # Centering the corrected inclination values around their median
    corrected_inclino = inclino - inclino.median()

    return corrected_inclino


# EMD and HHT, check also get_emd.py
def temp_drift_comp_emd(inclino: pd.Series, sample_rate: int = SAMPLE_RATE, freq_range: tuple = (
        LOW_FREQ_CUTOFF, HIGH_FREQ_CUTOFF)) -> pd.Series:  # Rust tuple = (0.05, 2, 128)

    import emd as emd  # Problems because reset of all loggers, only use insides of funktion

    # Wende EMD auf die Neigungswerte an
    imfs = emd.sift.sift(inclino.values)

    # Perform Hilbert-Huang Transform
    _, IF, _ = emd.spectra.frequency_transform(imfs, sample_rate, 'hilbert')  # -> IP, IF, IA

    # Filter IMFs within frequency range
    mask = (IF >= freq_range[0]) & (IF <= freq_range[1])
    filtered_imfs = np.where(mask[:, :, np.newaxis], imfs[:, :, np.newaxis], 0)

    # Reconstruct filtered data from IMFs
    data_filtered = np.sum(filtered_imfs, axis=1)

    # Create a new pd.Series with filtered data and correct index
    corrected_inclino = pd.Series(data_filtered.squeeze(), index=inclino.index)

    return corrected_inclino


def fft_freq_filter(inclino: pd.Series, sample_rate: int = SAMPLE_RATE,
                    freq_range: tuple = (0, HIGH_FREQ_CUTOFF)) -> pd.Series:
    """
    Filter frequencies outside the specified range using FFT after dropping NaN values.

    Parameters:
    - inclino: pd.Series containing the data to be filtered.
    - sample_rate: The sampling rate of the data in Hertz.
    - freq_range: A tuple defining the lower and upper frequency bounds.

    Returns:
    - pd.Series: Filtered data.
    """
    # Entfernen von NaN-Werten
    inclino_clean = inclino.dropna()

    # Fourier-Transformation der bereinigten Daten
    fft_data = np.fft.fft(inclino_clean.values)
    frequencies = np.fft.fftfreq(len(fft_data), d=1 / sample_rate)

    # Frequenzen außerhalb des gewünschten Bereichs auf null setzen
    mask = (frequencies < freq_range[0]) | (frequencies > freq_range[1])
    fft_data[mask] = 0

    # Anwenden der inversen Fourier-Transformation
    filtered_data = np.fft.ifft(fft_data)

    # Erstellen einer neuen pd.Series mit gefilterten Daten und korrektem Index
    filtered_inclino = pd.Series(filtered_data.real, index=inclino_clean.index)  # Verwenden nur des Realteils

    return filtered_inclino


def butter_lowpass_filter(inclino: pd.Series, sample_rate: int = SAMPLE_RATE,
                          cutoff_freq: int = HIGH_FREQ_CUTOFF) -> pd.Series:
    """
    Apply a Butterworth low-pass filter to the inclinometer data.

    Parameters:
    - inclino: pd.Series containing the inclinometer data to be filtered.
    - sample_rate: The sampling rate of the data in Hertz.
    - cutoff_freq: The cutoff frequency for the low-pass filter.

    Returns:
    - pd.Series: Filtered data.
    """
    # Ordnung des Filters und Frequenznormierung
    order = 5  # Kann angepasst werden für eine steilere Flanke
    nyquist = 0.5 * sample_rate
    normal_cutoff = cutoff_freq / nyquist

    # Butterworth-Filter Design
    b, a = butter(order, normal_cutoff, btype='low', analog=False)

    # Anwenden des Filters
    filtered_data = filtfilt(b, a, inclino.dropna().values)

    # Zurückgeben als pd.Series mit korrektem Index
    return pd.Series(filtered_data, index=inclino.dropna().index)
