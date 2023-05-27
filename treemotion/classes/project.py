# treemotion/classes/project.py

from common_imports.classes_heavy import *

logger = get_logger(__name__)


class Project(BaseClass):
    __tablename__ = 'Project'
    project_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
    location = Column(String)
    researcher = Column(String)
    datetime_start = Column(DateTime)

    series = relationship('Series', back_populates="project", lazy="joined", cascade='all, delete-orphan',
                          order_by='Series.series_id')

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

    @dec_runtime
    def load_from_csv(self, version: str = config.default_load_from_csv_version_name, overwrite: bool = False,
                      auto_commit: bool = True) -> Optional[List]:
        """
        Load data from CSV files for all series associated with the project.

        Args:
            version (str, optional): The version of the data to load_from_db.
            overwrite (bool, optional): Whether to overwrite existing data.
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.

        Returns:
            Optional[List]: A list of results from loading the data.
        """
        logger.info(f"Starting process to load_from_db all CSV files for {self.__str__()}")
        try:
            results = self.method_for_all_in_list('load_from_csv', version, overwrite, auto_commit)
        except Exception as e:
            logger.error(f"Error loading all CSV files for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of loading CSV files for {len(results)} series from {self.__str__()} successfully completed.")
        return results

    @dec_runtime
    def add_filenames(self, csv_path: str, auto_commit: bool = True):
        """
        Add filenames to all associated series.

        Args:
            csv_path (str): The path to the CSV files.
            auto_commit (bool, optional): From dec_auto_commit, If True, automatically commits the database session. Defaults to False.
        """
        result = self.method_for_all_in_list('add_filenames', csv_path=csv_path)
        if auto_commit:
            db_manager.commit()
        return result
