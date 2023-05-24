import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import emd
from treemotion.classes.measurement import Measurement

test_dir = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\py_tms_tools\test"
csv_file = r"C:\Users\mail\Meine Ablage\Kyellsen\005_Projekte\2023_Kronensicherung_Plesse\020_Daten\TMS\CSV_Messung_001_Plesse_export_2023-03-22_24h\2023-03-22 000000__DatasA000-0000-0007.csv"
db_file = r"C:\Users\mail\Meine Ablage\Kyellsen\006_Tools\py_tms_tools\test\test.db"

messung_1 = Measurement.read_from_csv(source_path=csv_file, messung_id=1, feedback=True)

df = messung_1.version

df = df.set_index(pd.DatetimeIndex(df['Time']))

print(df['Absolute-Inclination'].idxmax())
start_time = pd.Timestamp('2023-03-22 15:25:00')
end_time = pd.Timestamp('2023-03-22 15:30:00')
messung_1.limit_time(start_time, end_time)
dfk = messung_1.version

plt.plot(dfk['East-West-Inclination - drift compensated'])
plt.show()

sample_rate = 20
imf = emd.sift.sift(dfk['Absolute-Inclination'].values)
emd.plotting.plot_imfs(imf, scale_y=True, cmap=True)

# klassische Hilbert-Huang-Transformation (HHT)
IP, IF, IA = emd.spectra.frequency_transform(imf, sample_rate, 'hilbert')

freq_edges, freq_centres = emd.spectra.define_hist_bins(0, 1, 30)

freq_range = (0.05, 2, 128)
hht_f, spec = emd.spectra.hilberthuang(IF, IA, freq_range, scaling='density')

plt.figure()
plt.plot(hht_f, spec)
plt.legend(['Hilbert-Huang Transform'])
plt.xlim(0, 2)
plt.show()


plt.plot(imf[:, 2])
print("hier1")
imfy = emd.sift.sift(dfk['North-South-Inclination'].values)
imfx = emd.sift.sift(dfk['East-West-Inclination'].values)
print("hier2")
plt.figure(figsize=(12, 4))
plt.subplot(231)
plt.plot(imf[:, 1], imfy[:, 1], 'tab:green')

plt.subplot(232)
plt.plot(imf[:, 2], imfy[:, 2], 'tab:cyan')

plt.subplot(233)
plt.plot(imf[:, 3], imfy[:, 3], 'tab:purple')

plt.subplot(234)
plt.plot(imf[:, 4], imfy[:, 4], 'tab:red')

plt.subplot(235)
plt.plot(imf[:, 5], imfy[:, 5], 'tab:orange')

plt.subplot(236)
plt.plot(imf[:, 6], imfy[:, 6], 'tab:pink')
print("hier3")
plt.show()
print("hier4")

# Compute frequency statistics
IP, IF, IA = emd.spectra.frequency_transform(imfx, sample_rate, 'nht')  # normalisierte Hilbert-Huang-Transformation (NHT)

plt.figure(figsize=(12, 4))

plt.subplot(221)
# Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
plt.hist(IF[:, 2], np.linspace(0, 1), weights=IA[:, 2])
plt.grid(True)
plt.title('IF Histogram\nweighted by IA')
# plt.xticks(np.arange(0, 2, 5))
plt.xlabel('Frequency (Hz)')

plt.subplot(222)
# Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
plt.hist(IF[:, 3], np.linspace(0, .5), weights=IA[:, 3])
plt.grid(True)
plt.title('IF Histogram\nweighted by IA')
# plt.xticks(np.arange(0, 2, 5))
plt.xlabel('Frequency (Hz)')

plt.subplot(223)
# Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
plt.hist(IF[:, 4], np.linspace(0, .25), weights=IA[:, 4])
plt.grid(True)
plt.title('IF Histogram\nweighted by IA')
# plt.xticks(np.arange(0, 2, 5))
plt.xlabel('Frequency (Hz)')

plt.subplot(224)
# Plot an amplitude-weighted histogram using frequency bins from 0-20Hz
plt.hist(IF[:, 5], np.linspace(0, .125), weights=IA[:, 5])
plt.grid(True)
plt.title('IF Histogram\nweighted by IA')
# plt.xticks(np.arange(0, 2, 5))
plt.xlabel('Frequency (Hz)')
plt.show()
