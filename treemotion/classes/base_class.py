# treemotionen/classes/base_class.py
from utils.imports_classes import *
from utils.base import Base

logger = get_logger(__name__)


class BaseClass(Base):
    """
    This is the base class for other classes in the project.
    """
    __abstract__ = True  # mark class as abstract

    def __init__(self, *args, **kwargs):
        """
        Initialize the BaseClass instance.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        for attr, value in kwargs.items():
            setattr(self, attr, value)

    def for_all(self, list_name: str, method_name: str, *args, **kwargs):
        """
        Call a method on all objects in a list and return the results.

        Args:
            list_name (str): The name of the list attribute.
            method_name (str): The name of the method to be called.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            List: A list containing the return values of the method calls.
        """
        results = []  # Store the method return values here
        for obj in getattr(self, list_name):
            method = getattr(obj, method_name, None)
            if callable(method):
                result = method(*args, **kwargs)
                results.append(result)  # Append the method return value to the list
            else:
                logger.error(f"The method {method_name} does not exist in the class {obj.__class__.__name__}.")
                return None
        return results  # Return the list of method return values

    @classmethod
    @timing_decorator
    def load_from_db(cls, filter_by: Optional[Dict] = None, session: Optional[Session] = None) -> List:
        """
        Load objects of the class from the database.

        Args:
            filter_by (dict, optional): A dictionary of filtering criteria.
            session (Session, optional): The database session to use.

        Returns:
            List: A list of loaded objects.
        """
        session = db_manager.get_session(session)
        if filter_by is not None:
            objs = session.query(cls).filter_by(**filter_by).all()
        else:
            objs = session.query(cls).all()

        logger.info(f"{len(objs)} {cls.__name__} objects were successfully loaded.")
        return objs

    def copy(self, id_name: str = 'id', reset_id: bool = False, auto_commit: bool = False,
             session: Optional[Session] = None) -> 'BaseClass':
        """
        Create a copy of the instance.

        Args:
            id_name (str, optional): The name of the ID attribute.
            reset_id (bool, optional): Whether to reset the ID attribute of the new instance.
            auto_commit (bool, optional): Whether to automatically commit the new instance to the database.
            session (Session, optional): The database session to use.

        Returns:
            BaseClass: A new instance that is a copy of the current instance.
        """
        new_obj = self.__class__()

        for attr, value in self.__dict__.items():
            if attr == '_sa_instance_state':
                continue

            setattr(new_obj, attr, value)

        if reset_id:
            try:
                setattr(new_obj, id_name, None)
                logger.debug(f"Reset id attribute {id_name} in new instance.")
            except Exception as e:
                logger.error(f"Error resetting id attribute {id_name}: {e}")

        if auto_commit:
            setattr(new_obj, id_name, None)
            session = db_manager.get_session(session)
            try:
                session.add(new_obj)
                db_manager.commit(session)
                logger.info(f"New instance of {self.__class__.__name__} added to session and committed.")
            except Exception as e:
                session.rollback()
                logger.error(f"Error committing new instance of {self.__class__.__name__}: {e}")

        return new_obj

    def copy_deep(self, copy_relationships: bool = True) -> 'BaseClass':
        """
        Create a deep copy of the instance.

        Args:
            copy_relationships (bool, optional): Whether to copy relationships to other objects.

        Returns:
            BaseClass: A new instance that is a deep copy of the current instance.
        """
        new_obj = self.__class__()

        for attr, value in self.__dict__.items():
            if attr == '_sa_instance_state':
                continue

            try:
                if isinstance(value, list) and copy_relationships:
                    setattr(new_obj, attr, [item.copy() for item in value])
                    logger.debug(f"Copied list attribute {attr} from {self.__class__.__name__} to new instance.")
                elif isinstance(value, BaseClass) and copy_relationships:
                    setattr(new_obj, attr, value.copy())
                    logger.debug(f"Copied BaseClass attribute {attr} from {self.__class__.__name__} to new instance.")
                else:
                    setattr(new_obj, attr, value)
                    logger.debug(f"Copied attribute {attr} from {self.__class__.__name__} to new instance.")
            except Exception as e:
                logger.error(f"Error copying attribute {attr}: {e}")
                continue

        return new_obj

    def remove(self, id_name: str = 'id', auto_commit: bool = False, session: Optional[Session] = None) -> bool:
        """
        Remove the instance from the database.

        Args:
            id_name (str, optional): The name of the ID attribute.
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.
            session (Session, optional): The database session to use.

        Returns:
            bool: True if the instance was successfully removed, False otherwise.
        """
        session = db_manager.get_session(session)
        existing_obj = session.query(type(self)).get(getattr(self, id_name))
        try:
            if existing_obj is not None:
                session.delete(existing_obj)
                logger.info(f"Object {self.__class__.__name__} was removed.")
            else:
                logger.info(f"Object {self.__class__.__name__} does not exist.")
                return False
            if auto_commit:
                db_manager.commit(session)
            return True
        except Exception as e:
            session.rollback()  # Rollback the changes on error
            logger.error(f"Error removing the object {self.__class__.__name__}: {e}")
            return False
