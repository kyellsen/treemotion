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

    series = relationship(Series, backref="project", lazy="joined", cascade="all, delete, delete-orphan",
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
    @timing_decorator
    def load_from_db(cls, project_id=None, session=None) -> List['Project']:
        """
        Load projects from the database.

        Args:
            project_id (int, optional): The ID of the project to load.
            session (Session, optional): The database session to use.

        Returns:
            List[Project]: A list of loaded projects.
        """
        session = db_manager.get_session(session)
        objs = super().load_from_db(filter_by={'project_id': project_id} if project_id else None, session=session)
        logger.info(f"{len(objs)} projects were successfully loaded.")
        return objs

    @timing_decorator
    def load_data_from_csv(self, version: str = config.default_load_data_from_csv_version_name, overwrite: bool = False,
                           auto_commit: bool = False, session=None) -> Optional[List]:
        """
        Load data from CSV files for all series associated with the project.

        Args:
            version (str, optional): The version of the data to load.
            overwrite (bool, optional): Whether to overwrite existing data.
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.
            session (Session, optional): The database session to use.

        Returns:
            Optional[List]: A list of results from loading the data.
        """
        logger.info(f"Starting process to load all CSV files for {self.__str__()}")
        try:
            results = self.for_all('series', 'load_data_from_csv', version, overwrite, auto_commit, session)
        except Exception as e:
            logger.error(f"Error loading all CSV files for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of loading CSV files for {len(results)} series from {self.__str__()} successfully completed.")
        return results

    @timing_decorator
    def copy(self, reset_id: bool = False, auto_commit: bool = False, session=None) -> 'Project':
        """
        Create a copy of the project.

        Args:
            reset_id (bool, optional): Whether to reset the ID of the new instance.
            auto_commit (bool, optional): Whether to automatically commit the new instance to the database.
            session (Session, optional): The database session to use.

        Returns:
            Project: A new instance that is a copy of the current project.
        """
        new_obj = super().copy("project_id", reset_id, auto_commit, session)
        return new_obj

    @timing_decorator
    def remove(self, auto_commit: bool = False, session=None) -> bool:
        """
        Remove the project from the database.

        Args:
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.
            session (Session, optional): The database session to use.

        Returns:
            bool: True if the project was successfully removed, False otherwise.
        """
        session = db_manager.get_session(session)
        # Call the base class method to remove this Project object from the database
        result = super().remove('project_id', auto_commit, session)
        return result

    @timing_decorator
    def add_filenames(self, csv_path: str):
        """
        Add filenames to all associated series.

        Args:
            csv_path (str): The path to the CSV files.
        """
        self.for_all('series', 'add_filenames', csv_path=csv_path)

    def get_data_by_version(self, version: str) -> Optional[List]:
        """
        Get data instances for all associated series with the specified version.

        Args:
            version (str): The version of the data.

        Returns:
            Optional[List]: A list of data instances.
        """
        try:
            results = self.for_all('series', 'get_data_by_version', version)
        except Exception as e:
            logger.error(
                f"Error searching for data instances with version '{version}' from {self.__str__()}, Error: {e}")
            return None
        return results

    @timing_decorator
    def load_data_by_version(self, version: str, session=None) -> Optional[List]:
        """
        Load data frames for all associated series with the specified version.

        Args:
            version (str): The version of the data.
            session (Session, optional): The database session to use.

        Returns:
            Optional[List]: A list of loaded data frames.
        """
        logger.info(f"Starting process to load data frames in {self.__str__()} with version: {version}")
        try:
            results = self.for_all('series', 'load_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Error loading data frames for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of loading data frames for {len(results)} series from {self.__str__()} successfully completed.")
        return results

    @timing_decorator
    def copy_data_by_version(self, version_new: str = config.default_copy_data_by_version_name,
                             version_source: str = config.default_load_data_from_csv_version_name,
                             auto_commit: bool = False, session=None) -> Optional[List]:
        """
        Copy data objects for all associated series from the source version to the new version.

        Args:
            version_new (str, optional): The new version to copy the data to.
            version_source (str, optional): The source version to copy the data from.
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.
            session (Session, optional): The database session to use.

        Returns:
            Optional[List]: A list of results from copying the data.
        """
        logger.info(f"Starting process to copy all data objects in {self.__str__()} with version: {version_source}")
        try:
            results = self.for_all('series', 'copy_data_by_version', version_new, version_source, auto_commit,
                                   session)
        except Exception as e:
            logger.error(f"Error copying all data objects for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of copying all data objects for {len(results)} series from {self.__str__()} successfully completed.")
        return results

    @timing_decorator
    def commit_data_by_version(self, version: str, session=None) -> Union[List[bool], bool]:
        """
        Commit data objects for all associated series with the specified version.

        Args:
            version (str): The version of the data.
            session (Session, optional): The database session to use.

        Returns:
            Union[List[bool], bool]: A list of results indicating whether each data object was successfully committed.
        """
        logger.info(f"Starting process to commit all data objects in {self.__str__()} with version: {session}")
        try:
            results = self.for_all('series', 'commit_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Error committing all data objects for {self.__str__()}, Error: {e}")
            return False
        # Count the number of successful results (those that are not False)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Process of committing data objects for {successful}/{len(results)} series from {self.__str__()} successful.")
        return results

    @timing_decorator
    def limit_time_data_by_version(self, version: str, start_time: str, end_time: str, auto_commit: bool = False,
                                   session=None) -> Optional[List]:
        """
        Limit the time range of data objects for all associated series with the specified version.

        Args:
            version (str): The version of the data.
            start_time (str): The start time of the time range.
            end_time (str): The end time of the time range.
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.
            session (Session, optional): The database session to use.

        Returns:
            Optional[List]: A list of results from limiting the time range of the data objects.
        """
        logger.info(
            f"Starting process to limit time range of data objects in {self.__str__()} with version: {version}")
        try:
            results = self.for_all('series', 'limit_time_data_by_version', version, start_time, end_time,
                                   auto_commit, session)
        except Exception as e:
            logger.error(f"Error limiting time range of data objects for {self.__str__()}, Error: {e}")
            return None
        # Count the number of successful results (those that are not False)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Process of limiting time range of data objects for {successful}/{len(results)} series from {self.__str__()} successful.")
        return results
