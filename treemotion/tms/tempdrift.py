import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import emd as emd


def temp_drift_comp_lin_reg(data: pd.Series, temperature: pd.Series) -> pd.Series:
    # Subtrahieren der Medianwerte von Inklination
    data -= data.median()

    # Neigungswerte und Temperaturwerte in einem Dataframe zusammenfügen
    df = pd.concat([data, temperature], axis=1, keys=['data', 'temperature'])

    # Lineare Regression durchführen
    model = LinearRegression()
    model.fit(df[['temperature']], df['data'])

    # Korrekturfaktor berechnen
    correction_factor = model.coef_[0] * df['temperature'] + model.intercept_

    # Korrigierte Neigungswerte zurückgeben
    return data - correction_factor


def temp_drift_comp_lin_reg_2(data: pd.Series, temperature: pd.Series) -> pd.Series:
    # Subtrahieren der Medianwerte von Inklination
    data -= data.median()

    # Lineare Regression für Inklination in Abhängigkeit von der Temperatur
    coeff_1, coeff_2 = np.polyfit(temperature, data, deg=1)

    # Berechnung der Trendlinie
    trendline = temperature * coeff_1 + coeff_2

    # Korrigierte Neigungswerte zurückgeben
    return data - trendline


def temp_drift_comp_mov_avg(data: pd.Series, window_size: int) -> pd.Series:
    # Gleitenden Durchschnitt berechnen
    rolling_mean = data.rolling(window=window_size).mean()

    return data - rolling_mean


### EMD and HHT, check also get_emd.py

def temp_drift_comp_emd(data: pd.Series, sample_rate: int, freq_range: tuple) -> pd.Series:
    # Wende EMD auf die Neigungswerte an
    imfs = emd.sift.sift(data.values)

    # Perform Hilbert-Huang Transform
    _, IF, _ = emd.spectra.frequency_transform(imfs, sample_rate, 'hilbert') # -> IP, IF, IA

    # Filter IMFs within frequency range
    mask = (IF >= freq_range[0]) & (IF <= freq_range[1])
    filtered_imfs = np.where(mask[:, :, np.newaxis], imfs[:, :, np.newaxis], 0)

    # Reconstruct filtered data from IMFs
    data_filtered = np.sum(filtered_imfs, axis=1)

    # Create a new pd.Series with filtered data
    return pd.Series(data_filtered.squeeze())
