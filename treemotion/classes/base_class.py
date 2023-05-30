# treemotionen/classes/base_class.py
from typing import List
from sqlalchemy import inspect
# from copy import deepcopy # nur für auskommentiert deep_copy Methode
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
    def load_from_db(cls, ids: Optional[Union[int, List[int]]] = None) -> List[Any]:
        """
        Load instances of the own class from a SQLite database using SQLAlchemy.

        Parameters
        ----------
        ids : int, list of int, or None, optional
            The primary keys of the instances to load_from_db. If ids is None, all instances are loaded.
            Default is None.

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

            logger.info(f"Loading successful '{len(objs)}' of class {cls.__name__}.")
            return objs
        except Exception as e:
            logger.error(
                f"Failed to load instances of '{cls.__name__}' with primary keys '{ids}' from db. Error: '{e}'")
            raise

    @dec_runtime
    def load_from_csv(self, version_name: str = config.default_load_from_csv_version_name, overwrite: bool = False) -> \
            Optional[List]:
        """
        Load data from CSV files for all series associated with the project.

        Args:
            version_name (str, optional): The version of the data to load_from_db.
            overwrite (bool, optional): Whether to overwrite existing data.

        Returns:
            Optional[List]: A list of versions from loading the data.
        """
        logger.info(f"Starting process to load_from_db all CSV files for {self}")
        try:
            versions = self.method_for_all_in_list('load_from_csv', version_name, overwrite)
        except Exception as e:
            logger.error(f"Error loading all CSV files for {self}, Error: {e}")
            return None
        logger.info(
            f"Process of loading CSV files for {len(versions)} from {self} successfully completed.")
        return versions

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

    def find_all_of_class(self, class_name: str) -> List[Any]:
        """
        Find all instances of a given class within the hierarchy of the current instance.

        Args:
            class_name (str): The name of the class of which instances are to be found.

        Returns:
            List[Any]: A list containing all the found instances of the class.
        """
        if self.__class__.__name__ == class_name:
            return [self]

        found_instances: List[Any] = []
        children_instances = self.get_children_instances()

        if children_instances is not None:
            for child in children_instances:
                try:
                    found_child_instances = child.find_all_of_class(class_name)
                    found_instances.extend(found_child_instances)
                except Exception as e:
                    logger.error(
                        f"Error occurred while executing 'find_all_of_class' on '{child.__class__.__name__}': {e}")

        return found_instances

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

        instances_of_class = self.find_all_of_class(class_name)
        logger.info(f"Found '{len(instances_of_class)}' instances of class '{class_name}'.")

        for instance in instances_of_class:
            method = getattr(instance, method_name, None)
            if callable(method):
                try:
                    result = method(*args, **kwargs)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(
                        f"Error occurred while executing the method '{method_name}' on '{instance.__class__.__name__}': {e}"
                    )
            else:
                logger.error(f"The method '{method_name}' does not exist in the class '{instance.__class__.__name__}'.")

        return results

    @dec_runtime
    def get_versions_by_filter(self, filter_dict: Dict[str, Any], method: str = "list_filter") -> Optional[List[List]]:
        """
        Executes 'get_versions_by_filter' method in all 'Measurement' class children with given filter.

        :param filter_dict: Dictionary with filter keys and values
        :param method: The method to use for filtering. Possible values are "list_filter" and "db_filter".
               The default value is "list_filter".
               list_filter is way faster, but not searching in database.
        :return: List with versions_lists of method execution on all 'Measurement' class children,
                 or None if an error occurred.
        """
        logger.info(f"Start 'get_versions_by_filter' with filter '{filter_dict}' for instance of '{self}'")
        try:
            versions_lists = self.method_for_all_of_class(class_name="Measurement",
                                                          method_name='get_versions_by_filter',
                                                          filter_dict=filter_dict,
                                                          method=method)
            logger.info(f"Finished 'get_versions_by_filter' for instance of '{self}'")

            return versions_lists
        except Exception as e:
            logger.error(f"Error in '{self.__class__.__name__}'.get_versions_by_filter from '{self}', Error: {e}")
            return None

    @dec_runtime
    def get_versions_by_version_name(self, version_name: str = config.default_load_from_csv_version_name) -> \
            Optional[List[Any]]:
        """
        Executes 'get_versions_by_filter' method with 'version_name' filter in all 'Measurement' class children.

        :param version_name: Version name to use as filter
        :return: List with versions of method execution on all 'Measurement' class children from instance,
                 or None if an error occurred.
        """
        logger.info(f"Start 'get_versions_by_version_name' for '{self}'")
        versions = self.get_versions_by_filter({"version_name": version_name})

        logger.info(f"Finished 'get_versions_by_version_name' for '{self}'")

        return versions

    def copy(self, add_to_session: bool = True, auto_commit: bool = False) -> Optional['BaseClass']:
        """
        Create a shallow copy of this instance.

        Parameters
        ----------
        add_to_session : bool
            Whether to automatically add the new instance to the Session. Default is True.
        auto_commit : bool
            Whether to automatically commit the new instance to the database. Default is False.

        Returns
        -------
        BaseClass
            A new instance that is a shallow copy of the current instance.
        """
        logger.info(f"Start copy of '{self}'")
        try:
            cls = self.__class__
            primary_key = inspect(cls).primary_key[0].name

            copy = cls(**{attr: value for attr, value in self.__dict__.items() if attr != '_sa_instance_state'})
            setattr(copy, primary_key, None)  # set primary key = None

        except Exception as e:
            logger.error(f"Copy '{self}' failed ,error: {e}")
            return None

        if add_to_session:
            session = db_manager.get_session()
            session.add(copy)

        if auto_commit:
            logger.debug(f"Copy '{copy}' successful (auto_commit={auto_commit}).")
            db_manager.commit(session)
        else:
            logger.debug(f"Copy '{copy}' successful (auto_commit={auto_commit}).")
        return copy

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

    @staticmethod
    def get_attribute_from_instances(instances: List[Any], attribute_name: str) -> List[Any]:
        """
        Retrieves the given attribute for each instance in the provided list of instances.

        Args:
            instances (List[Any]): The list of instances from which the attribute should be retrieved.
            attribute_name (str): The name of the attribute to be retrieved.

        Returns:
            List[Any]: A list containing the values of the attribute for each instance.
        """
        attribute_values: List[Any] = []
        successful_retrievals = 0

        for instance in instances:
            attribute_value = getattr(instance, attribute_name, None)
            if attribute_value is not None:
                attribute_values.append(attribute_value)
                successful_retrievals += 1

        total_instances = len(instances)
        logger.info(
            f"Successfully retrieved '{attribute_name}' from '{successful_retrievals}/{total_instances}' instances.")

        return attribute_values

    def get_tms_dfs(self) -> Optional[List[Any]]:
        """
        Finds instances of the 'Version' class for the given object or list of objects and retrieves the attribute 'tms_df'
        for each of them.

        Returns
        -------
        Optional[List[Any]]
            The list of 'tms_df' attribute values for all found 'Version' instances.

        Raises
        ------
        Exception
            If an error occurs during the search of 'Version' instances or retrieval of 'tms_df' attributes.
        """
        try:
            version_instances: List[Any] = []
            objects = [self] if not isinstance(self, list) else self
            for obj in objects:
                found_versions = obj.find_all_of_class("Version")
                version_instances.extend(found_versions)

            logger.info(f"Found '{len(version_instances)}' instances of the 'Version' class")

            tms_dfs = self.get_attribute_from_instances(version_instances, "tms_df")

            return tms_dfs
        except Exception as e:
            logger.error(
                f"Failed to get 'tms_df' attributes from 'Version' instances. Error: '{e}'")
            return None

    # Auskommentiert, weil ids nicht richtig nach commit angepasst werden
    # def deep_copy(self, auto_commit: bool = False) -> 'BaseClass':
    #     """
    #     Create a deep copy of this instance along with its related instances.
    #
    #     Parameters
    #     ----------
    #     auto_commit : bool
    #         Whether to automatically commit the new instance to the database. Default is False.
    #
    #     Returns
    #     -------
    #     BaseClass
    #         A new instance that is a deep copy of the current instance.
    #     """
    #     logger.info(f"Start copy of '{self}'")
    #     cls = self.__class__
    #     primary_key = inspect(cls).primary_key[0].name
    #
    #     copied_attrs = deepcopy(self.__dict__)
    #     copied_attrs.pop(primary_key, None)  # exclude the primary key
    #     copied_attrs.pop('_sa_instance_state', None)  # exclude SQLAlchemy's instance state
    #
    #     # Create a new instance with the copied attributes
    #     new_instance = cls(**copied_attrs)
    #
    #     # Copy the related instances
    #     children = self.get_children_instances()
    #     if children:
    #         logger.info("Copying child instances")
    #         copied_children = [child.deep_copy() for child in children]
    #         attr_name = self.get_child_attr_name()
    #         if attr_name:
    #             setattr(new_instance, attr_name, copied_children)
    #
    #     session = db_manager.get_session()
    #     session.add(self)
    #
    #     if auto_commit:
    #         logger.debug(f"Auto committing the new instance '{new_instance}' to the database")
    #         db_manager.commit(session)
    #     else:
    #         logger.debug(f"Copy '{new_instance}' successful.")
    #
    #     return new_instance
