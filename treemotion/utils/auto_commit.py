from functools import wraps
from treemotion import db_manager
from .log import get_logger

logger = get_logger(__name__)


def auto_commit(method):
    """
    A decorator that adds automatic commit functionality to a method.

    Args:
        method (function): The method to decorate.

    Returns:
        function: The decorated method.

    """

    @wraps(method)
    def wrapper(*args, **kwargs):
        """
        Wrapper function that adds auto-commit functionality.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Keyword Args:
            auto_commit (bool, optional): If True, automatically commits the database session. Defaults to False.

        Returns:
            Any: The result of the decorated method.

        """
        auto_commit = kwargs.pop('auto_commit', False)
        result = method(*args, **kwargs)

        if auto_commit:
            session = db_manager.get_session()
            try:
                db_manager.commit(session)
            except Exception as e:
                logger.error(f"Auto commit failed: {e}")
                db_manager.rollback(session)
                raise

        return result

    return wrapper


def auto_commit_cls(method):
    """
    A decorator that adds automatic commit functionality to a class method returning objects.

    Args:
        method (function): The class method to decorate.

    Returns:
        function: The decorated class method.

    """
    @classmethod
    @wraps(method)
    def wrapper(cls, *args, **kwargs):
        """
        Wrapper function that adds auto-commit functionality to class methods returning objects.

        Args:
            cls (class): The class.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Keyword Args:
            auto_commit (bool, optional): If True, automatically commits the database session. Defaults to False.

        Returns:
            Any: The result of the decorated class method.

        """
        auto_commit = kwargs.pop('auto_commit', False)
        result = method(cls, *args, **kwargs)

        if auto_commit:
            session = db_manager.get_session()
            try:
                session.add(result)
                db_manager.commit(session)
                logger.info(f"New instance of {result.__class__.__name__} added to session and committed.")
            except Exception as e:
                session.rollback()
                logger.error(f"Error committing new instance of {result.__class__.__name__}: {e}")
                raise

        return result

    return wrapper
