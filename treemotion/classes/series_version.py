from typing import List

from .series import Series
from .measurement import Measurement
from .version import Version


class SeriesVersion(Series):
    def __init__(self, *args, series_version_id: int, series_id, version_name: str, get_tms_df: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.series_version_id = series_version_id
        self.version_name = version_name

        self.series = Series.load_from_db(ids=self.series_id)

        self.version_objs = self.series.get_by_version(version_name=self.version_name)

        if get_tms_df:
            self.apply_get_tms_df()


    #def get_measurement_objs =
    def apply_get_tms_df(self):
        self.version_objs = [obj.get_tms_df() for obj in self.version_objs]
        return self.version_objs
