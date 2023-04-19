import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import emd as emd

def plot_imfs(imfs, sample_rate):
    """
    Plots the Intrinsic Mode Functions (IMFs) of a given dataset using the EMD package.

    The function utilizes the plotting functionality of the EMD package to visualize
    the IMFs of a dataset, given its IMFs and the sample rate.

    Parameters
    ----------
    imfs : numpy.ndarray
        The Intrinsic Mode Functions (IMFs) of the dataset, typically obtained
        from the EMD package's Empirical Mode Decomposition (emd.sift.sift or emd.sift.ensemble_sift).
    sample_rate : int or float
        The sample rate of the original dataset.

    """
    # Plot the IMFs using EMD's plotting function
    emd.plotting.plot_imfs(imfs, sample_rate=sample_rate, scale_y=True, cmap=True)

    # Show the resulting plot
    plt.show()


def plot_hhs(hht_freq_centers, hht_spectrum):
    """
    Plots the Hilbert-Huang Transform (HHT) spectrum of a given dataset.

    The function visualizes the HHT spectrum by plotting the frequency centers
    against the HHT spectrum values.

    Parameters
    ----------
    hht_freq_centers : numpy.ndarray
        The frequency centers obtained from the Hilbert-Huang Transform.
    hht_spectrum : numpy.ndarray
        The HHT spectrum values obtained from the Hilbert-Huang Transform.

    """
    plt.figure()
    # Plot the HHT frequency centers against the spectrum values
    plt.plot(hht_freq_centers, hht_spectrum)
    plt.legend(['Hilbert-Huang Transform'])
    plt.xlim(0, 2)
    plt.show()
