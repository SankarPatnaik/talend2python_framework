"""Runtime utility helpers and error handling primitives."""

from __future__ import annotations

from typing import Any, Callable


class Talend2PyError(Exception):
    """Base exception for all errors raised during job execution."""


def handle_component_error(name: str, exc: Exception) -> None:
    """Raise a :class:`Talend2PyError` with component context.

    Parameters
    ----------
    name:
        Name of the component that failed.
    exc:
        The original exception encountered while executing the component.
    """

    raise Talend2PyError(f"Component {name} failed") from exc


def safe_call(func: Callable[..., Any], name: str, *args: Any, **kwargs: Any) -> Any:
    """Execute ``func`` and wrap any exception using ``handle_component_error``."""

    try:
        return func(*args, **kwargs)
    except Exception as e:  # pragma: no cover - thin wrapper
        handle_component_error(name, e)


__all__ = ["Talend2PyError", "handle_component_error", "safe_call"]

