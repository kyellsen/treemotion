# treemotionen/classes/base_class.py
from typing import Any

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

    def run_all(self, class_name: Optional[str] = None, method_name: Optional[str] = None, *args: Any, **kwargs: Any) -> \
            List[Any]:
        """
        Execute a method on all instances of a specified class and return the results.

        Args:
            class_name (str, optional): The name of the class on which the method should be called. Defaults to the class of self.
            method_name (str, optional): The name of the method to be called. Defaults to '__str__' method of the class.

        Returns:
            List[Any]: A list containing the return values of the method calls.
        """
        class_name = class_name or self.__class__.__name__
        method_name = method_name or '__str__'
        results: List[Any] = []

        if self.__class__.__name__ == class_name:
            method = getattr(self, method_name, None)
            if callable(method):
                try:
                    result = method(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    logger.error(
                        f"Error occurred while executing the method {method_name} on {self.__class__.__name__}: {e}")
            else:
                logger.error(f"The method {method_name} does not exist in the class {self.__class__.__name__}.")
        else:
            children = self.get_children()
            for child in children:
                try:
                    result = child.run_all(class_name, method_name, *args, **kwargs)
                    if result is not None:
                        results.extend(result)
                except Exception as e:
                    logger.error(
                        f"Error occurred while executing the method {method_name} on {child.__class__.__name__}: {e}")

        return results

    def get_children(self):
        # dictionary mapping the parent class to the name of the child attribute
        mapping = {
            "Project": "series",
            "Series": "measurement",
            "Measurement": "version",
        }

        # return the child instances
        return getattr(self, mapping.get(self.__class__.__name__), [])

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
        results = []
        for obj in getattr(self, list_name):
            method = getattr(obj, method_name, None)
            if callable(method):
                result = method(*args, **kwargs)
                results.append(result)  # Append the method return value to the list
            else:
                logger.error(f"The method {method_name} does not exist in the class {obj.__class__.__name__}.")
                return None
        return results

    @classmethod
    @dec_runtime
    def load_from_db(cls, filter_by: Optional[Dict] = None, ids: Optional[List[int]] = None) -> List:
        """
        Load objects of the class from the database.

        Args:
            filter_by (dict, optional): A dictionary of filtering criteria.
            ids (list of ints, optional): A list of ids.

        Returns:
            List: A list of loaded objects.
        """
        session = db_manager.get_session()
        query = session.query(cls)

        if filter_by is not None:
            query = query.filter_by(**filter_by)
        if ids is not None:
            # Assuming each class has a single primary key.
            primary_key = list(class_mapper(cls).primary_key)[0]
            query = query.filter(primary_key.in_(ids))

        objs = query.all()
        if not objs:
            logger.warning(f"No {cls.__name__} found.")

        logger.info(f"{len(objs)} {cls.__name__} objects were successfully loaded.")
        return objs

    def copy(self, reset_id: bool = False, auto_commit: bool = False) -> 'BaseClass':
        """
        Create a copy of the instance.

        Args:
            reset_id (bool, optional): Whether to reset the ID attribute of the new instance.
            auto_commit (bool, optional): Whether to automatically commit the new instance to the database.

        Returns:
            BaseClass: A new instance that is a copy of the current instance.
        """
        new_obj = self.__class__()

        for attr, value in self.__dict__.items():
            if attr == '_sa_instance_state':
                continue

            setattr(new_obj, attr, value)

        primary_key_attr = self.__mapper__.primary_key[0].name

        if reset_id:
            try:
                setattr(new_obj, primary_key_attr, None)
                logger.debug(f"Reset id attribute {primary_key_attr} in new instance.")
            except Exception as e:
                logger.error(f"Error resetting id attribute {primary_key_attr}: {e}")

        if auto_commit:
            setattr(new_obj, primary_key_attr, None)
            session = db_manager.get_session()
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

    def remove(self, auto_commit: bool = False) -> bool:
        """
        Remove the instance from the database.

        :param auto_commit:

        Returns:
            bool: True if the instance was successfully removed, False otherwise.
        """
        session = db_manager.get_session()
        primary_key_attr = self.__mapper__.primary_key[0].name
        existing_obj = session.get(type(self), getattr(self, primary_key_attr))
        try:
            if existing_obj is not None:
                session.delete(existing_obj)
                logger.info(f"Object {self.__class__.__name__} was removed.")
            else:
                logger.info(f"Object {self.__class__.__name__} does not exist.")
                return False

        except Exception as e:
            logger.error(f"Error removing the object {self.__class__.__name__}: {e}")
            return False
        if auto_commit:
            db_manager.auto_commit(self.__class__.__name__, "remove")
        return True
