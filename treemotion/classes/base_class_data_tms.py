import numpy as np
import pandas as pd
from typing import Type, Dict, Tuple, List, Optional, Union, Any

from kj_core.df_utils.time_cut import validate_time_format, time_cut_by_datetime_index
from kj_core.plotting.multiple_dfs import plot_multiple_dfs

from ..common_imports.imports_classes import *
from ..classes import BaseClass
from kj_logger import get_logger

from ..tms.find_peaks import *
from ..tms.tempdrift import temp_drift_comp_lin_reg, temp_drift_comp_lin_reg_2, temp_drift_comp_mov_avg, \
    temp_drift_comp_emd
from ..tms.tempdrift import fft_freq_filter, butter_lowpass_filter
from ..tms.inclination import calc_abs_inclino, calc_inclination_direction

logger = get_logger(__name__)


class BaseClassDataTMS(BaseClass):
    __abstract__ = True

    tempdrift_methods = {
        "original": None,
        "linear": temp_drift_comp_lin_reg,
        "linear_2": temp_drift_comp_lin_reg_2,
        "moving_average": temp_drift_comp_mov_avg,
        "emd": temp_drift_comp_emd,
    }

    filter_methods = {
        "no_filter": None,
        "butter_lowpass": butter_lowpass_filter,
        "fft": fft_freq_filter,
    }

    def __init__(self, data: pd.DataFrame = None, measurement_version_id: int = None, tempdrift_method: str = None,
                 filter_method: str = None):
        super().__init__()
        self.data = data
        self.measurement_version_id = measurement_version_id
        self.tempdrift_method = tempdrift_method
        self.filter_method = filter_method

    def correct_tms_data(self, method: str = "linear", freq_filter: str = "butter_lowpass", inplace: bool = False,
                         auto_commit=False, **kwargs: Any) -> pd.DataFrame:
        """
        Corrects temperature distortion of inclination data using a specified method and optionally updates
        the instance's data attribute in place.

        Parameters:
        - method (str, optional): Method for temperature drift compensation. Defaults to "linear".
        - freq_filter (str, optional):
        - inplace (bool, optional): If True, updates the instance's data attribute. Defaults to True.
        - **kwargs: Keyword arguments for the temperature drift compensation function.

        - Allowed functions are:
            - linear: temp_drift_comp_lin_reg(inclino, temperature: pd.Series) - default
            - linear_2: temp_drift_comp_lin_reg_2(inclino, temperature: pd.Series)
            - moving_average: temp_drift_comp_mov_avg(inclino, window_size=1000) = 6 minutes
            - emd: temp_drift_comp_emd(inclino, sample_rate=20, freq_range=(0.05, 2, 128))

        Returns:
        - pd.DataFrame: Corrected data.

        Raises:
        - ValueError: If an unauthorized method is passed.
        """

        if method not in self.tempdrift_methods:
            logger.error(f"Method {method} for temperature drift compensation is not allowed.")
            raise ValueError(f"Unauthorized method {method} for temperature drift compensation.")

        temp_drift_comp_func = self.tempdrift_methods[method]

        if freq_filter not in self.filter_methods:
            logger.error(f"Method {freq_filter} for temperature drift compensation is not allowed.")
            raise ValueError(f"Unauthorized method {freq_filter} for temperature drift compensation.")

        filter_func = self.filter_methods[freq_filter]

        if method in ["linear", "linear_2"]:
            kwargs["temperature"] = self.data["Temperature"]

        data_copy = self.data.copy()

        try:
            for axis in ['East-West-Inclination', 'North-South-Inclination']:
                inclino_series = data_copy[axis]
                compensated = temp_drift_comp_func(inclino_series, **kwargs)
                if filter_func and method in ["linear", "linear_2", "moving_average"]:
                    compensated = filter_func(inclino=compensated)
                data_copy[f"{axis} - drift compensated"] = compensated

            data_copy = self.calc_inclino_abs_and_dir(data_copy)

            if inplace:
                self.data = data_copy
                self.tempdrift_method = method
                self.filter_method = freq_filter

            if auto_commit:
                self.get_database_manager().commit()

            logger.info(
                f"Temperature drift compensation successfully performed. Method: {method}, freq_filter: {freq_filter}, inplace: {inplace}")
            return data_copy
        except Exception as e:
            logger.error(f"Error in performing temperature drift compensation: {e}")
            raise

    @staticmethod
    def calc_inclino_abs_and_dir(data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the total inclination and direction based on compensated data.

        Parameters:
        - data: DataFrame containing East-West and North-South inclination data.

        Returns:
        - DataFrame: The input DataFrame with added columns for absolute inclination
                     and inclination direction, both drift compensated.
        """
        data['Absolute-Inclination - drift compensated'] = calc_abs_inclino(
            data['East-West-Inclination - drift compensated'],
            data['North-South-Inclination - drift compensated'])

        data['Inclination direction of the tree - drift compensated'] = calc_inclination_direction(
            data['East-West-Inclination - drift compensated'],
            data['North-South-Inclination - drift compensated'])
        return data

    def compare_tms_tempdrift_methods(self):
        results = {"original": self.data.copy()}

        for method in ["linear", "moving_average", "emd"]:  # "linear_2",
            if method in ["linear", "linear_2", "moving_average"]:
                for freq_filter in ["no_filter", "butter_lowpass", "fft"]:
                    compensated_data = self.correct_tms_data(method=method, freq_filter=freq_filter, inplace=False)
                    results[f"{method}_{freq_filter}"] = compensated_data
            elif method in ["emd"]:
                compensated_data = self.correct_tms_data(method=method, freq_filter="no_filter", inplace=False)
                results[f"{method}"] = compensated_data

        return results

    def plot_compare_tms_tempdrift(self, start_time='2022-01-29T19:30:00', end_time='2022-01-29T20:00:00'):
        results = self.compare_tms_tempdrift_methods()

        original_columns = ['East-West-Inclination',
                            'North-South-Inclination',
                            'Absolute-Inclination',
                            'Temperature',
                            ]

        compensated_columns = ['East-West-Inclination - drift compensated',
                               'North-South-Inclination - drift compensated',
                               'Absolute-Inclination - drift compensated'
                               ]

        dfs_and_columns: [List[Tuple[str, pd.DataFrame, List[str]]]] = [
            ("Original", results["original"], original_columns)
        ]

        for method, df in results.items():
            if method != "original":
                df_time = df.loc[start_time:end_time].copy()
                dfs_and_columns.append((method, df_time, compensated_columns))

        fig = plot_multiple_dfs(dfs_and_columns)
        plot_manager = self.get_plot_manager()
        filename = f'mv_id_{self.measurement_version_id}'
        subdir=f"tempdrift/compare_tms_tempdrift/{self.__class__.__name__}"

        plot_manager.save_plot(fig, filename, subdir)

    def plot_tms_tempdrift(self):
        pass

    def random_sample(self, n: int, inplace: bool = False, auto_commit: bool = False) -> pd.DataFrame:
        """
        Selects a random sample of data while preserving the original order of the selected data points.

        Parameters
        ----------
        n : int
            Number of data points to select.
        inplace : bool, default True
            If True, updates the object's data in place. Otherwise, returns a new DataFrame.
        auto_commit : bool, default False
            If True, automatically commits changes to the database (if applicable).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the randomly selected data points.

        Raises
        ------
        ValueError
            If an error occurs during sample selection.
        """

        logger.info(f"Selecting a random sample of {n} data points.")

        if n > len(self.data):
            logger.warning(
                f"The requested sample size ({n}) exceeds the data length ({len(self.data)}). Using all data.")
            n = len(self.data)

        try:
            # Sample without replacement, sort indices to preserve order
            sampled_indices = self.data.sample(n, random_state=None).index
            sampled_indices_sorted = sorted(sampled_indices)

            # Use loc to maintain the datetime index
            sampled_data = self.data.loc[sampled_indices_sorted]

            if inplace:
                self.data = sampled_data

            if auto_commit:
                self.get_database_manager().commit()

            logger.debug("Random sample selected successfully.")
            return sampled_data
        except Exception as e:
            logger.error(f"Failed to select a random sample: {e}")

    def time_cut(self, start_time: str, end_time: str, inplace: bool = False, auto_commit: bool = False) -> Union[pd.DataFrame, None]:
        """
        Limits the data to a specific time range and optionally updates the instance data in-place.

        Parameters
        ----------
        start_time : str
            The start time of the range, in a format compatible with `validate_time_format`.
        end_time : str
            The end time of the range, in a format compatible with `validate_time_format`.
        inplace : bool, optional
            If True, updates the instance's data in-place. Defaults to False.
        auto_commit : bool, optional
            If True, automatically commits changes to the database. Defaults to False.

        Returns
        -------
        Version
            Self-reference for method chaining.
        """
        # Validate time formats
        validated_start_time = validate_time_format(start_time)
        validated_end_time = validate_time_format(end_time)

        if isinstance(validated_start_time, ValueError) or isinstance(validated_end_time, ValueError):
            logger.error("Invalid time format provided.")
            return

        # Attempt to limit data within the specified time range
        try:
            data = self.data.copy()
            data = time_cut_by_datetime_index(data,  start_time=start_time, end_time=end_time)

            if inplace:
                self.data = data

            if auto_commit:
                self.get_database_manager().commit()

            logger.info(f"Successfully limited the data of '{self}' between '{start_time}' and '{end_time}', inplace: '{inplace}', auto_commit: '{auto_commit}'.")

            return data
        except Exception as e:
            logger.error(f"Error limiting the data of '{self}': {e}")
            return
#
#
#
# # @property
# # def peak_max(self) -> Optional[Dict]:
# #
# #     datetime_column_name = self.datetime_column_name
# #     main_value_column_name = self.get_config().Data.main_tms_value
# #     try:
# #         peak = find_max_peak(self.data, main_value_column_name, datetime_column_name)
# #     except Exception as e:
# #         logger.warning(f"No peak found for {self}, error: {e}")
# #         return None
# #
# #     # For debugging
# #     show_peak: bool = False
# #     if show_peak:
# #         logger.info(f"Peak in {self}: {peak}")
# #     return peak
# #
# # @property
# # def peaks(self):
# #     config = self.get_config().Data
# #     datetime_column_name = self.datetime_column_name
# #     tms_main_value = self.get_config().Data.main_tms_value
# #     n_peaks: int = config.n_peaks
# #     sample_rate: float = config.sample_rate
# #     min_time_diff: float = config.min_time_diff
# #     prominence: int = config.prominence
# #
# #     try:
# #         peaks = find_n_peaks(self.data, tms_main_value, datetime_column_name, n_peaks, sample_rate, min_time_diff,
# #                              prominence)
# #     except Exception as e:
# #         logger.warning(f"No peaks found for {self.__str__()}, error: {e}")
# #         return None
# #
# #     # For debugging
# #     show_peaks: bool = False
# #     if show_peaks:
# #         logger.info(f"Peaks found in {self.__str__()}: {peaks.__str__()}")
# #     return peaks
