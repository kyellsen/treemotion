import pandas as pd


# PROBLEM WITH LOADING package dcor version > 4.0, its part of EMD

# Perform Hilbert-Huang Transform
## klassische Hilbert-Huang-Transformation (hilbert)
## normalisierte Hilbert-Huang-Transformation (nht)


def get_imfs(data: pd.Series):
    import emd as emd
    return emd.sift.sift(data.values)


def get_hht(imfs, sample_rate: float):
    import emd as emd
    IP, IF, IA = emd.spectra.frequency_transform(imfs, sample_rate, 'hilbert')
    return IP, IF, IA


def get_hht_spectrum(IF, IA, freq_range, sample_rate):
    import emd as emd
    hht_freq_centers, hht_spectrum = emd.spectra.hilberthuang(IF, IA, edges=freq_range, sample_rate=sample_rate,
                                                              scaling='density')
    return hht_freq_centers, hht_spectrum
