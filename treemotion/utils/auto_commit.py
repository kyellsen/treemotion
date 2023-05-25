from functools import wraps
from typing import Callable, Any
from treemotion import db_manager
from .log import get_logger

logger = get_logger(__name__)


def dec_auto_commit(method: Callable[..., Any]) -> Callable[..., Any]:
    """
    A decorator that wraps a method with optional auto_commit functionality.

    Args:
        method (Callable[..., Any]): The function to decorate.

    Returns:
        Callable[..., Any]: The decorated function.
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs) -> Any:
        """
        Wrapper function that adds auto-commit functionality.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Keyword Args:
            auto_commit (Optional[bool], optional): If True, automatically commits the database session. Defaults to False.

        Returns:
            Any: The result of the decorated method.
        """
        auto_commit = kwargs.pop('auto_commit', False)
        result = method(self, *args, **kwargs)

        if auto_commit:
            session = None
            try:
                if isinstance(result, tuple):
                    result, session = result
                else:
                    session = db_manager.get_session()

                if auto_commit:
                    db_manager.commit(session)
                    logger.debug(f"Auto commit successful for method {method.__name__}")
                return result

            except Exception as e:
                db_manager.rollback(session)
                logger.error(f"Auto commit failed for method {method.__name__}: {e}")
                raise
    return wrapper
