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
        return f"Project(id={self.project_id}, name={self.name})"

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
