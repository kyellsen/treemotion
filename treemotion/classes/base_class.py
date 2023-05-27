# treemotionen/classes/base_class.py
from typing import Any
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


    @dec_runtime
    @classmethod
    def load(cls, ids: Optional[Union[int, List[int]]] = None, filter_by: Optional[Dict] = None,
             get_tms_df: bool = False):
        """
        Load instances of the class from the database, filtered by the provided criteria.

        Args:
            ids (Optional[Union[int, List[int]]]): An id or list of object ids to load.
            filter_by (Optional[Dict]): A dictionary of filtering criteria.
            get_tms_df (bool): If set to True, calls get_tms_df() on Version instances related to the loaded objects.

        Returns:
            None, cls, List[cls]: If no objects are found, returns None if a single id is provided, else an empty list.
                If objects are found, returns a single object if a single id is provided, else a list of objects.
        """
        logger.info(f"Start loading instance(s) of '{cls.__name__}' from the database.")
        session = db_manager.get_session()
        query = cls._build_query(session, ids, filter_by)
        objs = query.all()
        if not objs:
            logger.warning(f"No instances of '{cls.__name__}' found with provided criteria.")
            return None if isinstance(ids, int) else []

        num_objs = len(objs)
        logger.info(f"'{num_objs}' instance(s) of '{cls.__name__}' successfully loaded from the database.")

        if get_tms_df:
            cls._apply_get_tms_df(objs)

        return objs[0] if isinstance(ids, int) else objs

    @classmethod
    def _build_query(cls, session: Session, ids: Optional[Union[int, List[int]]] = None,
                     filter_by: Optional[Dict] = None):
        """
        Constructs a SQLAlchemy Query object based on provided criteria.

        Args:
            session (Session): SQLAlchemy Session object.
            ids (Optional[Union[int, List[int]]]): An id or list of object ids to load.
            filter_by (Optional[Dict]): A dictionary of filtering criteria.

        Returns:
            Query: The constructed SQLAlchemy Query object.
        """
        query = session.query(cls)

        if ids is not None:
            if isinstance(ids, int):
                ids = [ids]
            primary_key = list(class_mapper(cls).primary_key)[0]
            query = query.filter(primary_key.in_(ids))

        if filter_by is not None:
            query = query.filter_by(**filter_by)

        return query

    @classmethod
    def _apply_get_tms_df(cls, objs: List[Any]) -> int:
        """
        Applies get_tms_df method on the loaded objects, sets the result to obj._tms_df
        and counts the number of successful operations.

        Args:
            objs (List[cls]): The list of objects to apply get_tms_df.

        Returns:
            int: The number of successful get_tms_df operations.
        """
        count_version_objs = 0
        count_get_tms_df_success = 0
        logger.info(f"Start Version.get_tms_df.")
        for obj in objs:
            if cls.__name__ == 'Version':
                tms_df = obj.get_tms_df()
                count_version_objs += 1
                if tms_df is not None:  # check if the return value is not None
                    obj.set_tms_df(tms_df, update_metadata=True)
                    count_get_tms_df_success += 1
            else:
                for result in obj.method_for_all_of_class(class_name='Version', method_name='get_tms_df'):
                    count_version_objs += 1
                    if result is not None:
                        obj._tms_df = result
                        count_get_tms_df_success += 1
        logger.info(
            f"Version.get_tms_df successfully applied on '{count_get_tms_df_success}/{count_version_objs}' instance(s) in '{cls.__name__}'.")

        return count_get_tms_df_success

    def get_children(self) -> Optional[List[Any]]:
        """
        Get the child instances of the current class.

        Returns:
            List[Any]: A list of child instances if the attribute name is found, otherwise None.
        """
        mapping = {
            "Project": "series",
            "Series": "measurement",
            "Measurement": "version",
        }

        attr_name = mapping.get(self.__class__.__name__)
        return getattr(self, attr_name, None) if attr_name else None

    def method_for_all_in_list(self, method_name: Optional[str], *args: Any, **kwargs: Any) -> List[Any]:
        """
        Call a method on all objects in a list and return the results.

        Args:
            method_name (str): The name of the method to be called.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            List[Any]: A list containing the return values of the method calls.
        """

        children = self.get_children()
        if not children:
            logger.error("No child instances found.")
            return []
        logger.info(f"Applying '{method_name}' to '{len(children)}' of class '{children[0].__class__.__name__}'!")
        results: List[Any] = []

        for obj in children:
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
            children = self.get_children()
            if children is not None:
                for child in children:
                    try:
                        result = child.method_for_all_of_class(class_name, method_name, *args, **kwargs)
                        results.extend(result)
                    except Exception as e:
                        logger.error(
                            f"Error occurred while executing the method '{method_name}' on '{child.__class__.__name__}': {e}"
                        )
        return results

    @dec_runtime
    def get_version_by(self, filter_dict: Dict[str, Any]) -> Optional[List[Any]]:
        """
        Executes 'get_version_by' method in all 'Measurement' class children with given filter.

        :param filter_dict: Dictionary with filter keys and values
        :return: List with version_objs of method execution on all 'Measurement' class children,
                 or None if an error occurred.
        """
        logger.info(f"Start 'get_version_by' with filter '{filter_dict}' for instance of '{self}'")
        try:
            version_objs = self.method_for_all_of_class(class_name="Measurement", method_name='get_version_by',
                                                        filter_dict=filter_dict)
            logger.info(f"Finished 'get_version_by' for instance of '{self}'")
            return version_objs
        except Exception as e:
            logger.error(f"Error in '{self.__class__.__name__}'.get_version_by from '{self}', Error: {e}")
            return None

    @dec_runtime
    def get_by_version(self, version_name: str = config.default_load_from_csv_version_name) -> \
            Optional[List[Any]]:
        """
        Executes 'get_version_by' method with 'version_name' filter in all 'Measurement' class children.

        :param version_name: Version name to use as filter
        :return: List with version_objs of method execution on all 'Measurement' class children,
                 or None if an error occurred.
        """
        logger.info(f"Start 'get_by_version' for '{self}'")
        version_objs = self.get_version_by({"version_name": version_name})

        logger.info(f"Finished 'get_by_version' for '{self}'")

        return version_objs

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

    @dec_runtime
    def delete_from_db(self, auto_commit=False) -> bool:
        """
        Delete the instance from the database.
        :param auto_commit:

        Returns:
            bool: True if the instance was successfully deleted, False otherwise.
        """
        session = db_manager.get_session()
        session.delete(self)

        if auto_commit:
            db_manager.commit(session)
        logger.info(f"Object {self.__str__()} was deleted.")
        return True
