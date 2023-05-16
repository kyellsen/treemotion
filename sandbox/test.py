import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import emd


def plot_amplitude_weighted_histogram(IF, IA, bins=np.linspace(0, 1, 21)):

    plt.figure()

    plt.subplot(2, 2, 1)
    # Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
    plt.hist(IF[:, 2], np.linspace(0, 1), weights=IA[:, 2])
    plt.grid(True)
    plt.xlabel('Frequency (Hz)')

    plt.subplot(2, 2, 2)
    # Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
    plt.hist(IF[:, 3], np.linspace(0, .5), weights=IA[:, 3])
    plt.grid(True)
    plt.xlabel('Frequency (Hz)')

    plt.subplot(2, 2, 3)
    # Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
    plt.hist(IF[:, 4], np.linspace(0, .25), weights=IA[:, 4])
    plt.grid(True)
    plt.xlabel('Frequency (Hz)')

    plt.subplot(2, 2, 4)
    # Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
    plt.hist(IF[:, 5], np.linspace(0, .125), weights=IA[:, 5])
    plt.grid(True)
    plt.xlabel('Frequency (Hz)')
    plt.show()

def filter_data(data: pd.Series, sample_rate: int = 20, freq_range: tuple = (0.05, 2, 128)) -> pd.Series:
    # Perform EMD
    imf = emd.sift.sift(data.values)

    # Plot IMFs
    plot_imfs(imf)

    # Perform Hilbert-Huang Transform
    IP, IF, IA = perform_hht(imf, sample_rate)

    # Compute Hilbert-Huang Spectrum
    hht_f, spec = compute_hhs(IF, IA, freq_range)

    # Plot Hilbert-Huang Spectrum
    plot_hhs(hht_f, spec)

    # Plot Input Data
    plot_data(data, 'Input Data')

    # Filter IMFs within frequency range
    mask = (IF >= freq_range[0]) & (IF <= freq_range[1])
    filtered_imfs = np.where(mask[:, :, np.newaxis], imf[:, :, np.newaxis], 0)

    # Reconstruct filtered data from IMFs
    filtered_data = np.sum(filtered_imfs, axis=1)

    # Plot Filtered Data
    plot_data(pd.Series(filtered_data.squeeze(), index=data.index), 'Filtered Data')

    # Plot Amplitude-weighted Histogram
    plot_amplitude_weighted_histogram(IF, IA)

    # Create a new pd.Series with filtered data
    filtered_series = pd.Series(filtered_data.squeeze(), index=data.index)

    return filtered_series


def filter_data_2(data: pd.Series, sample_rate: int = 20, freq_range: tuple = (0.05, 2)) -> pd.Series:
    # Perform EMD
    imf = emd.sift.sift(data.values)

    # Perform Hilbert-Huang Transform
    IP, IF, IA = emd.spectra.frequency_transform(imf, sample_rate, 'hilbert')

    # Filter IMFs within frequency range
    mask = (IF >= freq_range[0]) & (IF <= freq_range[1])
    filtered_imfs = np.where(mask[:, :, np.newaxis], imf[:, :, np.newaxis], 0)

    # Reconstruct filtered data from IMFs
    filtered_data = np.sum(filtered_imfs, axis=1)

    # Create a new pd.Series with filtered data
    return pd.Series(filtered_data.squeeze())
