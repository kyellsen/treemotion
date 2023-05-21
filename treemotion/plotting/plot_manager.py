from pathlib import Path
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.io as pio


class PlotManager:
    def __init__(self, folder_path, figsize=(8, 6), dpi=300, style='default', grid=True, wide_format=False):
        self.folder_path = Path(folder_path)
        self.folder_path.mkdir(parents=True, exist_ok=True)  # Erstelle den Ordner, falls er noch nicht existiert.

        # Setzen der initialen Plot-Attribute
        self.figsize = figsize
        self.dpi = dpi
        self.style = style
        self.grid = grid
        self.wide_format = wide_format

    def save_matplotlib_plot(self, fig, filename):
        """Speichern Sie ein Matplotlib-Diagramm als jpg-Datei"""
        full_path = self.folder_path / f"{filename}.jpg"
        fig.savefig(str(full_path), dpi=self.dpi)

    def save_plotly_plot(self, fig, filename):
        """Speichern Sie ein Plotly-Diagramm als html-Datei"""
        full_path = self.folder_path / f"{filename}.html"
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
