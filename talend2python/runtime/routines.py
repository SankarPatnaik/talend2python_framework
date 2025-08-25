"""Registry for user defined routine functions.

Talend jobs often rely on custom Java routines.  The Python framework mirrors
this by allowing users to register arbitrary callables which are then exposed to
generated jobs.  Routines can be registered either via the :func:`routine`
decorator or by calling :func:`register` directly.
"""

from __future__ import annotations

from typing import Callable, Dict


registry: Dict[str, Callable] = {}


def register(name: str, func: Callable) -> None:
    """Register ``func`` under ``name`` in the routine registry."""

    registry[name] = func


def routine(name: str) -> Callable[[Callable], Callable]:
    """Decorator variant of :func:`register`.  Example::

        @routine("MyRoutine")
        def my_routine(x):
            return x * 2
    """

    def deco(fn: Callable) -> Callable:
        register(name, fn)
        return fn

    return deco


__all__ = ["registry", "register", "routine"]

