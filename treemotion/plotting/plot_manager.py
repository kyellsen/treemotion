from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.io as pio


class PlotManager:
    def __init__(self, figsize=(8, 6), dpi=300, style='default', grid=True, wide_format=False):

        self.plot_directory = None
        self.set_plot_directory()

        # Setzen der initialen Plot-Attribute
        self.figsize = figsize
        self.dpi = dpi
        self.style = style
        self.grid = grid
        self.wide_format = wide_format

    def set_plot_directory(self):
        from treemotion import config
        self.plot_directory = config.plot_directory
        self.plot_directory.mkdir(parents=True, exist_ok=True)

    def save_matplotlib_plot(self, fig, filename):
        """Speichern Sie ein Matplotlib-Diagramm als jpg-Datei"""
        full_path = self.plot_directory / f"{filename}.jpg"
        fig.savefig(str(full_path), dpi=self.dpi)

    def save_plotly_plot(self, fig, filename):
        """Speichern Sie ein Plotly-Diagramm als html-Datei"""
        full_path = self.plot_directory / f"{filename}.html"
        pio.write_html(fig, str(full_path))

    def apply_attributes(self, plot_type):
        """Anwenden der festgelegten Attribute auf den Plot"""
        if plot_type == 'matplotlib':
            plt.rcParams['figure.figsize'] = self.figsize
            plt.rcParams['axes.grid'] = self.grid
            plt.rcParams['savefig.dpi'] = self.dpi
            plt.rcParams['figure.autolayout'] = self.wide_format
            plt.style.use(self.style)
        elif plot_type == 'plotly':
            # Hier können Sie entsprechende Anpassungen vornehmen, um Plotly-spezifische Attribute festzulegen
            pass
        else:
            raise ValueError("Unsupported plot type. Please use 'matplotlib' or 'plotly'.")
