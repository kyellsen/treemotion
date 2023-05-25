# treemotion/classes/project.py

from utils.imports_classes import *

from .series import Series

logger = get_logger(__name__)


class Project(BaseClass):
    __tablename__ = 'Project'
    project_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
    location = Column(String)
    researcher = Column(String)
    datetime_start = Column(DateTime)

    series = relationship(Series, back_populates="project", lazy="joined", cascade="all, delete, delete-orphan",
                          order_by=Series.series_id)

    def __init__(self, *args, project_id=None, name=None, location=None, researcher=None,
                 datetime_start=None, **kwargs):
        """
        Initialize the Project instance.

        Args:
            *args: Variable length argument list.
            project_id (int): The ID of the project.
            name (str): The name of the project.
            location (str): The location of the project.
            researcher (str): The researcher associated with the project.
            datetime_start (datetime): The start date and time of the project.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        # in SQLite Database
        self.project_id = project_id
        self.name = name
        self.location = location
        self.researcher = researcher
        self.datetime_start = datetime_start

    def __str__(self):
        """
        Get a string representation of the Project instance.

        Returns:
            str: A string representation of the Project instance.
        """
        return f"Project(id={self.project_id}, name={self.name}"

    @classmethod
    @dec_runtime
    def load_from_db(cls, project_id: Optional[Union[int, List[int]]] = None) -> List['Project']:
        if isinstance(project_id, list):
            objs = super().load_from_db(ids=project_id)
        else:
            objs = super().load_from_db(filter_by={'project_id': project_id} if project_id else None)
        return objs

    @dec_runtime
    def load_from_csv(self, version: str = config.default_load_from_csv_version_name, overwrite: bool = False,
                      auto_commit: bool = True) -> Optional[List]:
        """
        Load data from CSV files for all series associated with the project.

        Args:
            version (str, optional): The version of the data to load.
            overwrite (bool, optional): Whether to overwrite existing data.
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.

        Returns:
            Optional[List]: A list of results from loading the data.
        """
        logger.info(f"Starting process to load all CSV files for {self.__str__()}")
        try:
            results = self.for_all('series', 'load_from_csv', version, overwrite, auto_commit)
        except Exception as e:
            logger.error(f"Error loading all CSV files for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of loading CSV files for {len(results)} series from {self.__str__()} successfully completed.")
        return results

    @dec_runtime
    def add_filenames(self, csv_path: str, auto_commit: bool = False):
        """
        Add filenames to all associated series.

        Args:
            csv_path (str): The path to the CSV files.
            auto_commit (bool, optional): From dec_auto_commit, If True, automatically commits the database session. Defaults to False.
        """
        result = self.for_all('series', 'add_filenames', csv_path=csv_path)
        if auto_commit:
            db_manager.auto_commit(self.__class__.__name__, "add_filenames")
        return result

    # def get_data_by_version(self, version: str) -> Optional[List]:
    #     """
    #     Get data instances for all associated series with the specified version.
    #
    #     Args:
    #         version (str): The version of the data.
    #
    #     Returns:
    #         Optional[List]: A list of data instances.
    #     """
    #     try:
    #         results = self.for_all('series', 'get_data_by_version', version)
    #     except Exception as e:
    #         logger.error(
    #             f"Error searching for data instances with version '{version}' from {self.__str__()}, Error: {e}")
    #         return None
    #     return results
    #
    # @dec_runtime
    # def load_data_by_version(self, version: str) -> Optional[List]:
    #     """
    #     Load data frames for all associated series with the specified version.
    #
    #     Args:
    #         version (str): The version of the data.
    #
    #     Returns:
    #         Optional[List]: A list of loaded data frames.
    #     """
    #     logger.info(f"Starting process to load data frames in {self.__str__()} with version: {version}")
    #     try:
    #         results = self.for_all('series', 'load_data_by_version', version)
    #     except Exception as e:
    #         logger.error(f"Error loading data frames for {self.__str__()}, Error: {e}")
    #         return None
    #     logger.info(
    #         f"Process of loading data frames for {len(results)} series from {self.__str__()} successfully completed.")
    #     return results
    #
    # @dec_runtime
    # def copy_data_by_version(self, version_new: str = config.default_copy_data_by_version_name,
    #                          version_source: str = config.default_load_from_csv_version_name,
    #                          auto_commit: bool = False, session=None) -> Optional[List]:
    #     """
    #     Copy data objects for all associated series from the source version to the new version.
    #
    #     Args:
    #         version_new (str, optional): The new version to copy the data to.
    #         version_source (str, optional): The source version to copy the data from.
    #         auto_commit (bool, optional): Whether to automatically commit the changes to the database.
    #         session (Session, optional): The database session to use.
    #
    #     Returns:
    #         Optional[List]: A list of results from copying the data.
    #     """
    #     logger.info(f"Starting process to copy all data objects in {self.__str__()} with version: {version_source}")
    #     try:
    #         results = self.for_all('series', 'copy_data_by_version', version_new, version_source, auto_commit,
    #                                session)
    #     except Exception as e:
    #         logger.error(f"Error copying all data objects for {self.__str__()}, Error: {e}")
    #         return None
    #     logger.info(
    #         f"Process of copying all data objects for {len(results)} series from {self.__str__()} successfully completed.")
    #     return results
    #
    # @dec_runtime
    # def commit_data_by_version(self, version: str, session=None) -> Union[List[bool], bool]:
    #     """
    #     Commit data objects for all associated series with the specified version.
    #
    #     Args:
    #         version (str): The version of the data.
    #         session (Session, optional): The database session to use.
    #
    #     Returns:
    #         Union[List[bool], bool]: A list of results indicating whether each data object was successfully committed.
    #     """
    #     logger.info(f"Starting process to commit all data objects in {self.__str__()} with version: {session}")
    #     try:
    #         results = self.for_all('series', 'commit_data_by_version', version, session)
    #     except Exception as e:
    #         logger.error(f"Error committing all data objects for {self.__str__()}, Error: {e}")
    #         return False
    #     # Count the number of successful results (those that are not False)
    #     successful = sum(1 for result in results if result is not False)
    #     logger.info(
    #         f"Process of committing data objects for {successful}/{len(results)} series from {self.__str__()} successful.")
    #     return results
    #
    # @dec_runtime
    # def limit_time_data_by_version(self, version: str, start_time: str, end_time: str, auto_commit: bool = False,
    #                                session=None) -> Optional[List]:
    #     """
    #     Limit the time range of data objects for all associated series with the specified version.
    #
    #     Args:
    #         version (str): The version of the data.
    #         start_time (str): The start time of the time range.
    #         end_time (str): The end time of the time range.
    #         auto_commit (bool, optional): Whether to automatically commit the changes to the database.
    #         session (Session, optional): The database session to use.
    #
    #     Returns:
    #         Optional[List]: A list of results from limiting the time range of the data objects.
    #     """
    #     logger.info(
    #         f"Starting process to limit time range of data objects in {self.__str__()} with version: {version}")
    #     try:
    #         results = self.for_all('series', 'limit_time_data_by_version', version, start_time, end_time,
    #                                auto_commit, session)
    #     except Exception as e:
    #         logger.error(f"Error limiting time range of data objects for {self.__str__()}, Error: {e}")
    #         return None
    #     # Count the number of successful results (those that are not False)
    #     successful = sum(1 for result in results if result is not False)
    #     logger.info(
    #         f"Process of limiting time range of data objects for {successful}/{len(results)} series from {self.__str__()} successful.")
    #     return results
