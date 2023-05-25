from functools import wraps
from typing import Callable, Any
from treemotion import db_manager


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
            db_manager.auto_commit(class_name={result.__class__.__name__}, method_name={method.__name__})
        return result
    return wrapper


def dec_auto_commit_cls(method: Callable[..., Any]) -> Callable[..., Any]:
    """
    A decorator that wraps a classmethod with optional auto_commit functionality.

    Args:
        method (Callable[..., Any]): The function to decorate.

    Returns:
        Callable[..., Any]: The decorated function.
    """

    @wraps(method)
    def wrapper(cls, *args, **kwargs) -> Any:
        """
        Wrapper function that adds auto-commit functionality.

        Args:
            cls: The class that owns the method.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Keyword Args:
            auto_commit (Optional[bool], optional): If True, automatically commits the database session. Defaults to False.

        Returns:
            Any: The result of the decorated method.
        """
        auto_commit = kwargs.pop('auto_commit', False)
        result = method(cls, *args, **kwargs)

        if auto_commit:
            db_manager.auto_commit(class_name={cls.__name__}, method_name={method.__name__})

        return result
    return wrapper
