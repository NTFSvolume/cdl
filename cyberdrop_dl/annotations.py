from __future__ import annotations

from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    _P = ParamSpec("_P")
    _R = TypeVar("_R")
    _T = TypeVar("_T")
    import functools
    import inspect
    from collections.abc import Callable

    def copy_signature(target: Callable[_P, _R]) -> Callable[[Callable[..., _T]], Callable[_P, _T]]:
        def decorator(func: Callable[..., _T]) -> Callable[_P, _T]:
            @functools.wraps(func)
            def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
                return func(*args, **kwargs)

            wrapper.__signature__ = inspect.signature(target).replace(  # pyright: ignore[reportAttributeAccessIssue]
                return_annotation=inspect.signature(func).return_annotation
            )
            return wrapper

        return decorator

else:

    def copy_signature(_):
        def call(y):
            return y

        return call


_repr_templs: dict[type, str] = {}


def _make_repr_templ(*fields: str) -> str:
    return "{self.__class__.__qualname__}(" + ", ".join(f"{f}={{self.{f}!r}}" for f in fields) + ")"


def _get_templ(self: object) -> str:
    if templ := _repr_templs.get(type(self)):
        return templ

    elif hasattr(self, "__dict__"):
        names = tuple(vars(self))
    else:
        names: tuple[str, ...] = tuple(self.__slots__)

    templ = _repr_templs[type(self)] = _make_repr_templ(*names)
    return templ


def auto_repr(*fields: str):
    """Add a dataclass-style __repr__ to a slotted or normal class."""

    def _decorator(cls_: type[_T]) -> type[_T]:
        if fields:
            _repr_templs[cls_] = _make_repr_templ(*fields)

        def repr(self: _T) -> str:
            return _get_templ(self).format(self)

        cls_.__repr__ = repr
        return cls_

    return _decorator
