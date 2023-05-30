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

    def __init__(self, *args, **kwargs):
        super().__init__(*args)

        # in SQLite Database
        self.project_id = kwargs.get('project_id', None)
        self.name = kwargs.get('name', None)
        self.location = kwargs.get('location', None)
        self.researcher = kwargs.get('researcher', None)
        self.datetime_start = kwargs.get('datetime_start', None)

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
        result = self.method_for_all_children('add_filenames', csv_path=csv_path)
        if auto_commit:
            db_manager.commit()
        return result
