from ..common_imports.imports_classes import *

from .series import Series

logger = get_logger(__name__)

logger.info("TEAST")  # TODO: DELETE!


class Project(BaseClass):
    __tablename__ = 'Project'
    project_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    project_name = Column(String)
    location = Column(String)
    researcher = Column(String)
    datetime_start = Column(DateTime)

    series = relationship(Series, backref="project", lazy="joined", cascade='all, delete-orphan',
                          order_by='Series.series_id')

    def __init__(self, project_id=None, project_name=None, location=None, researcher=None, datetime_start=None):
        super().__init__()

        # in SQLite Database
        self.project_id = project_id
        self.project_name = project_name
        self.location = location
        self.researcher = researcher
        self.datetime_start = datetime_start

    def __str__(self):
        """
        Get a string representation of the Project instance.

        Returns:
            str: A string representation of the Project instance.
        """
        return f"Project(id={self.project_id}, name={self.project_name})"
