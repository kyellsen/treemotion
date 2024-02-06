import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import linregress
from sklearn.metrics import r2_score


def plot_wind_shift(measurement_id: int, merged_df: pd.DataFrame, shifted_df: pd.DataFrame,
                    tms_col: str, wind_col: str,
                    shift_sec: float, corr_shift_0: float, max_corr: float):
    # Erstellen Sie ein Figure- und Axes-Objekt
    fig, ax1 = plt.subplots()

    # TMS-Daten plotten
    ax1.plot(merged_df.index, merged_df[tms_col], color='tab:blue', label=f'{tms_col} [°]')
    ax1.set_xlabel('datetime')
    ax1.set_ylabel(tms_col, color='tab:blue')

    # Zweite Achse für Winddaten
    ax2 = ax1.twinx()
    ax2.plot(merged_df.index, merged_df[wind_col], color='tab:red', label=f'{wind_col} [m/s]')
    ax2.plot(shifted_df.index, shifted_df[wind_col], color='tab:orange', label=f'shifted_{wind_col} [m/s]')
    ax2.set_ylabel(wind_col, color='tab:red')

    # Legenden anzeigen
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc='upper left')

    # Textbox hinzufügen
    param_str = f"Optimal Shift: {shift_sec} [sec]\n" \
                f"Initial Correlation: {corr_shift_0:.4f}\n" \
                f"Max Correlation: {max_corr:.4f}"

    plt.title(f"Wind Shift for measurement '{measurement_id}'")
    plt.annotate(param_str, xy=(0.95, 0.10), xycoords='axes fraction', fontsize=10,
                 bbox=dict(boxstyle="round,pad=0.3", facecolor='white', edgecolor='black'),
                 verticalalignment='top', horizontalalignment='right')

    return fig


def plot_wind_shift_reg_linear(measurement_id: int, df: pd.DataFrame, wind_col: str, tms_col: str):
    df_clean = df.dropna(subset=[tms_col, wind_col])
    # Berechnung der linearen Regression
    slope, intercept, r_value, p_value, std_err = linregress(df_clean[wind_col], df_clean[tms_col])

    # Erstellen Sie ein Figure- und Axes-Objekt
    fig, ax = plt.subplots()

    # Datenpunkte plotten
    ax.scatter(df_clean[wind_col], df_clean[tms_col], color="black", label='Points', s=1)

    # Regressionslinie plotten
    x_vals = np.linspace(df_clean[wind_col].min(), df_clean[wind_col].max(), 100) #df_clean[wind_col].max()
    y_vals = intercept + slope * x_vals
    ax.plot(x_vals, y_vals, color='tab:red', label='Lineare Regression')

    # Beschriftungen und Titel
    ax.set_xlabel(f"{wind_col} [m/s]")
    ax.set_ylabel(f"{tms_col} [°]")
    ax.set_title(f"Linear Reg. measurement '{measurement_id}' wind_max vs. tms_max")

    # Legende anzeigen
    ax.legend()

    # Textbox hinzufügen
    param_str = f"Slope: {slope:.2f}\n" \
                f"Intercept: {intercept:.2f}\n" \
                f"R-Value: {r_value:.2f}\n" \
                f"P-Value: {p_value:.2g}\n" \
                f"Std Err: {std_err:.2f}"
    plt.annotate(param_str, xy=(0.95, 0.05), xycoords='axes fraction', fontsize=10,
                 bbox=dict(boxstyle="round,pad=0.3", facecolor='white', edgecolor='black'),
                 verticalalignment='bottom', horizontalalignment='right')

    # Rückgabe des Figure-Objekts
    return fig


def plot_wind_shift_reg_exp(measurement_id: int, df: pd.DataFrame, wind_col: str, tms_col: str):
    df_clean = df.dropna(subset=[tms_col, wind_col])

    # Exponentialfunktion berechnen
    ln_y = np.log(df_clean[tms_col])
    coeffs = np.polyfit(df_clean[wind_col], ln_y, 1)
    a = np.exp(coeffs[1])
    b = coeffs[0]

    # Berechnete Werte und Gütemaße
    y_pred = a * np.exp(b * df_clean[wind_col])
    r2 = r2_score(df_clean[tms_col], y_pred)

    # Erstellen Sie ein Figure- und Axes-Objekt
    fig, ax = plt.subplots()

    # Datenpunkte plotten
    ax.scatter(df_clean[wind_col], df_clean[tms_col], color="black", label='Points', s=1)

    # Exponentialfunktion plotten
    x_vals = np.linspace(df_clean[wind_col].min(), df_clean[wind_col].max(), 100) #instate 30 m/s as max df_clean[wind_col].max()
    y_vals = a * np.exp(b * x_vals)
    ax.plot(x_vals, y_vals, color='tab:red', label='Exponential Regression')

    # Beschriftungen und Titel
    ax.set_xlabel(f"{wind_col} [m/s]")
    ax.set_ylabel(f"{tms_col} [°]")
    ax.set_title(f"Exponential Reg. measurement: '{measurement_id}' wind_max vs. tms_max")

    # Legende anzeigen
    ax.legend()

    # Funktion und Gütemaße in einer Textbox anzeigen
    exp_func_str = f"y = {a:.2f} * exp({b:.2f} * x)"
    param_str = (f"Function: {exp_func_str}\n"
                 f"R^2: {r2:.2f}")
    plt.annotate(param_str, xy=(0.95, 0.05), xycoords='axes fraction', fontsize=10,
                 bbox=dict(boxstyle="round,pad=0.3", facecolor='white', edgecolor='black'),
                 verticalalignment='bottom', horizontalalignment='right')

    # Rückgabe des Figure-Objekts
    return fig
