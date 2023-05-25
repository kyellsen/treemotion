from functools import wraps
from typing import Callable, Any
from sqlalchemy.orm import Session
from treemotion import db_manager
from .log import get_logger

logger = get_logger(__name__)



def dec_auto_commit(method: Callable[..., Any]) -> Callable[..., Any]:
    """
    A decorator that adds automatic commit functionality to a method.

    Args:
        method (Callable[..., Any]): The method to decorate.

    Returns:
        Callable[..., Any]: The decorated method with auto-commit functionality.

    """
    @wraps(method)
    def wrapper(*args, **kwargs) -> Any:
        """
        Wrapper function that adds auto-commit functionality.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Keyword Args:
            auto_commit (bool, optional): If True, automatically commits the database session. Defaults to False.
            session (Session, optional): The database session to commit. If not provided, it's retrieved from the manager.

        Returns:
            Any: The result of the decorated method.

        """
        auto_commit = kwargs.get('auto_commit', False)
        session = kwargs.get('session') or db_manager.get_session()

        try:
            result = method(*args, **kwargs)
        except Exception as e:
            logger.error(f"An error occurred during method execution: {e}")
            raise

        if auto_commit:
            try:
                session.commit()
                logger.info(f"{method.__name__} committed successfully.")
            except Exception as e:
                logger.error(f"Auto commit failed: {e}")
                session.rollback()
                raise

        return result

    return wrapper
