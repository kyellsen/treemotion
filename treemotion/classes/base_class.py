# treemotionen/classes/base_class.py
from typing import List
from sqlalchemy import inspect
from copy import deepcopy
from common_imports.classes_heavy import *
from utils.base import Base

logger = get_logger(__name__)


class BaseClass(Base):
    """
    This is the base class for other classes in the project.
    """
    __abstract__ = True  # mark class as abstract

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    @dec_runtime
    def load_from_db(cls, ids: Optional[Union[int, List[int]]] = None, get_tms_df: bool = False) -> List[Any]:
        """
        Load instances of the own class from a SQLite database using SQLAlchemy.

        Parameters
        ----------
        ids : int, list of int, or None, optional
            The primary keys of the instances to load_from_db. If ids is None, all instances are loaded.
            Default is None.
        get_tms_df :

        Returns
        -------
        list of instances
            The loaded instances. If only one instance is loaded, it is returned inside a list.
            If no instances are found, an empty list is returned.
        """
        session: Session = db_manager.get_session()

        try:
            # Get the name of the primary key attribute (could be id, project_id, series_id, etc.)
            primary_key = inspect(cls).primary_key[0].name

            if ids is None:
                logger.info(f"Loading all instances of '{cls.__name__}'")
                objs = session.query(cls).all()
            elif isinstance(ids, int):
                logger.info(f"Loading instance of '{cls.__name__}' with primary key '{ids}'")
                instance = session.query(cls).filter(getattr(cls, primary_key) == ids).one_or_none()
                objs = [instance] if instance else []
            elif isinstance(ids, list):
                logger.info(f"Loading instances of '{cls.__name__}' with primary keys '{ids}'")
                objs = session.query(cls).filter(getattr(cls, primary_key).in_(ids)).all()
            else:
                raise ValueError('Invalid type of ids. Must be int, list of int, or None.')

            if not objs:
                logger.warning(f"No instances of '{cls.__name__}' with primary keys '{ids}' found")

            if get_tms_df:
                cls.apply_get_tms_df(objs)

            return objs
        except Exception as e:
            logger.error(
                f"Failed to load instances of '{cls.__name__}' with primary keys '{ids}' from db. Error: '{e}'")
            raise

    def get_child_attr_name(self) -> Optional[str]:
        """
        Get the attribute name of the children based on the class name.

        Returns
        -------
        str or None
            The attribute name if the class name is found, otherwise None.
        """
        mapping = {
            "Project": "series",
            "Series": "measurement",
            "Measurement": "version",
        }

        # Store the attribute name corresponding to the class in a variable
        child_attr_name = mapping.get(self.__class__.__name__)

        return child_attr_name

    def get_children_instances(self) -> Optional[List[Any]]:
        """
        Get the child instances of the current class based on the attribute name returned by get_child_attr_name.

        Returns
        -------
        list of any type or None
            A list of child instances if the attribute name is found, otherwise None.
        """
        # Get the attribute name of children using the helper function
        attr_name = self.get_child_attr_name()

        # Retrieve the child instances using the attribute name
        child_instances = getattr(self, attr_name, None) if attr_name else None

        return child_instances

    @staticmethod
    def apply_get_tms_df(objs: List[Any]) -> bool:
        """
        Applies Version.get_tms_df method on the list of loaded objects, sets the result to obj._tms_df,
        and counts the number of successful operations.

        Parameters
        ----------
        objs : list of any type
            The list of objects to apply get_tms_df.

        Returns
        -------
        bool
            True if the operation is successful, False otherwise.
        """
        logger.info("Start Version.get_tms_df.")
        count_version_objs = 0
        count_get_tms_df_success = 0
        try:
            for obj in objs:
                for result in obj.method_for_all_of_class(class_name='Version', method_name='get_tms_df'):
                    count_version_objs += 1
                    if result is not None:
                        obj._tms_df = result
                        count_get_tms_df_success += 1
            logger.info(
                f"Version.get_tms_df successfully applied on '{count_get_tms_df_success}/{count_version_objs}' instance(s).")
            return True
        except Exception as e:
            logger.error("Failed to apply Version.get_tms_df. Error: %s", e)
            return False

    def method_for_all_in_list(self, method_name: Optional[str], *args: Any, **kwargs: Any) -> List[Any]:
        """
        Call a method on all objects in an attribute list and return the results.

        Args:
            method_name (str): The name of the method to be called.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            List[Any]: A list containing the return values of the method calls.
        """

        children_instances = self.get_children_instances()
        if not children_instances:
            logger.error("No child instances found.")
            return []
        logger.info(
            f"Applying '{method_name}' to '{len(children_instances)}' of class '{children_instances[0].__class__.__name__}'!")
        results: List[Any] = []

        for obj in children_instances:
            method = getattr(obj, method_name, None)
            if callable(method):
                try:
                    result = method(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    logger.error(
                        f"Error occurred while executing the method '{method_name}' on '{obj.__class__.__name__}': {e}"
                    )
            else:
                logger.error(f"The method '{method_name}' does not exist in the class '{obj.__class__.__name__}'.")
                return []

        return results

    def method_for_all_of_class(self, class_name: Optional[str] = None, method_name: Optional[str] = None, *args: Any,
                                **kwargs: Any) -> List[Any]:
        """
        Execute a method on all instances of a specified class and return the results.

        Args:
            class_name (str, optional): The name of the class on which the method should be called. Defaults to the class of self.
            method_name (str, optional): The name of the method to be called. Defaults to '__str__' method of the class.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

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
                        f"Error occurred while executing the method '{method_name}' on '{self.__class__.__name__}': {e}"
                    )
            else:
                logger.error(f"The method '{method_name}' does not exist in the class '{self.__class__.__name__}'.")
        else:
            children_instances = self.get_children_instances()
            if children_instances is not None:
                for child in children_instances:
                    try:
                        result = child.method_for_all_of_class(class_name, method_name, *args, **kwargs)
                        results.extend(result)
                    except Exception as e:
                        logger.error(
                            f"Error occurred while executing the method '{method_name}' on '{child.__class__.__name__}': {e}"
                        )
        return results

    @dec_runtime
    def get_version_by_filter(self, filter_dict: Dict[str, Any]) -> Optional[List[Any]]:
        """
        Executes 'get_version_by_filter' method in all 'Measurement' class children with given filter.

        :param filter_dict: Dictionary with filter keys and values
        :return: List with version_objs of method execution on all 'Measurement' class children,
                 or None if an error occurred.
        """
        logger.info(f"Start 'get_version_by_filter' with filter '{filter_dict}' for instance of '{self}'")
        try:
            version_objs = self.method_for_all_of_class(class_name="Measurement", method_name='get_version_by_filter',
                                                        filter_dict=filter_dict)
            logger.info(f"Finished 'get_version_by_filter' for instance of '{self}'")
            return version_objs
        except Exception as e:
            logger.error(f"Error in '{self.__class__.__name__}'.get_version_by_filter from '{self}', Error: {e}")
            return None

    @dec_runtime
    def get_version(self, version_name: str = config.default_load_from_csv_version_name) -> \
            Optional[List[Any]]:
        """
        Executes 'get_version_by_filter' method with 'version_name' filter in all 'Measurement' class children.

        :param version_name: Version name to use as filter
        :return: List with version_objs of method execution on all 'Measurement' class children,
                 or None if an error occurred.
        """
        logger.info(f"Start 'get_version' for '{self}'")
        version_objs = self.get_version_by_filter({"version_name": version_name})

        logger.info(f"Finished 'get_version' for '{self}'")

        return version_objs

    def reset_id_attr(self, primary_key: str) -> None:
        """
        Resets the primary key attribute of an instance.
        :param primary_key: Name of the primary key attribute.
        """
        try:
            setattr(self, primary_key, None)
            logger.debug(f"Reset id attribute {primary_key} in new instance.")
        except Exception as e:
            logger.error(f"Error resetting id attribute {primary_key}: {e}")
            raise

    def copy(self, reset_id: bool = False, auto_commit: bool = False) -> 'BaseClass':
        """
        Create a shallow copy of this instance.

        Parameters
        ----------
        reset_id : bool
            Whether to reset the ID attribute of the new instance. Default is False.
        auto_commit : bool
            Whether to automatically commit the new instance to the database. Default is False.

        Returns
        -------
        BaseClass
            A new instance that is a shallow copy of the current instance.
        """
        logger.info(f"Start copy of '{self}'")
        cls = self.__class__
        primary_key = inspect(cls).primary_key[0].name

        new_instance = cls(**{attr: value for attr, value in self.__dict__.items() if attr != '_sa_instance_state'})

        if reset_id:
            logger.info("Resetting id for the new instance")
            new_instance.reset_id_attr(primary_key)

        session = db_manager.get_session()
        session.add(self)

        if auto_commit:
            logger.debug(f"Auto committing the new instance '{new_instance}' to the database")
            db_manager.commit(session)
        else:
            logger.debug(f"Copy '{new_instance}' successful.")
        return new_instance

    def deep_copy(self, auto_commit: bool = False) -> 'BaseClass':
        """
        Create a deep copy of this instance along with its related instances.

        Parameters
        ----------
        auto_commit : bool
            Whether to automatically commit the new instance to the database. Default is False.

        Returns
        -------
        BaseClass
            A new instance that is a deep copy of the current instance.
        """
        logger.info(f"Start copy of '{self}'")
        cls = self.__class__
        primary_key = inspect(cls).primary_key[0].name

        copied_attrs = deepcopy(self.__dict__)
        copied_attrs.pop(primary_key, None)  # exclude the primary key
        copied_attrs.pop('_sa_instance_state', None)  # exclude SQLAlchemy's instance state

        # Create a new instance with the copied attributes
        new_instance = cls(**copied_attrs)

        # Copy the related instances
        children = self.get_children_instances()
        if children:
            logger.info("Copying child instances")
            copied_children = [child.deep_copy() for child in children]
            attr_name = self.get_child_attr_name()
            if attr_name:
                setattr(new_instance, attr_name, copied_children)

        session = db_manager.get_session()
        session.add(self)

        if auto_commit:
            logger.debug(f"Auto committing the new instance '{new_instance}' to the database")
            db_manager.commit(session)
        else:
            logger.debug(f"Copy '{new_instance}' successful.")

        return new_instance

    @dec_runtime
    def delete_from_db(self, auto_commit=False) -> bool:
        """
        Delete the instance from the database.
        :param auto_commit:
        :returns: True if the instance was successfully deleted, False otherwise.
        """
        session = db_manager.get_session()
        session.delete(self)

        if auto_commit:
            db_manager.commit(session)
        logger.info(f"Object {self.__str__()} was deleted.")
        return True
