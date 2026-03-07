"""Async versions of builtins and some path operations"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import shutil
import sys
import tempfile
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Coroutine, Iterable, Iterator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from stat import S_ISREG
from typing import IO, TYPE_CHECKING, Any, AnyStr, Generic, ParamSpec, Self, TypeVar, cast, final, overload
from weakref import WeakValueDictionary

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence
    from types import TracebackType

    from _typeshed import OpenBinaryMode, OpenTextMode


_T = TypeVar("_T")
_P = ParamSpec("_P")
_R = TypeVar("_R")
_ExitT_co = TypeVar("_ExitT_co", bound="bool | None")


@dataclasses.dataclass(slots=True, eq=False)
class WeakAsyncLocks(Generic[_T]):
    """A WeakValueDictionary wrapper for asyncio.Locks.

    Unused locks are automatically garbage collected. When trying to retrieve a
    lock that does not exists, a new lock will be created.
    """

    _locks: WeakValueDictionary[_T, asyncio.Lock] = dataclasses.field(init=False, default_factory=WeakValueDictionary)

    def __getitem__(self, key: _T, /) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            self._locks[key] = lock = asyncio.Lock()
        return lock


async def gather(coros: Sequence[Awaitable[_T]], batch_size: int = 10) -> list[_T]:
    """Like `asyncio.gather`, but creates tasks lazily to minimize event loop overhead.

    This function ensures there are never more than `batch_size` tasks created at any given time.

    If any exception is raised within a task, all currently running tasks
    are cancelled and any renaming task in the queue will be ignored.
    """

    semaphore = asyncio.BoundedSemaphore(batch_size)
    results: list[_T] = cast("list[_T]", [None] * len(coros))

    async def worker(index: int, coro: Awaitable[_T]):
        try:
            result = await coro
            results[index] = result
        finally:
            semaphore.release()

    async with asyncio.TaskGroup() as tg:
        for index, coro in enumerate(coros):
            await semaphore.acquire()
            tg.create_task(worker(index, coro))

    return results


def to_thread(fn: Callable[_P, _R]) -> Callable[_P, Coroutine[None, None, _R]]:
    """Convert a blocking callable into an async callable that runs in another thread"""

    async def call(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        return await asyncio.to_thread(fn, *args, **kwargs)

    return call


@dataclasses.dataclass(slots=True, eq=False)
class AsyncIO(Generic[AnyStr]):
    """An asynchronous context manager wrapper for a file object."""

    _coro: Awaitable[IO[AnyStr]]
    _io: IO[AnyStr] = dataclasses.field(init=False)

    async def __aenter__(self) -> Self:
        self._io = await self._coro
        return self

    async def __aexit__(self, *_) -> None:
        return await asyncio.to_thread(self._io.close)

    async def __aiter__(self) -> AsyncIterator[AnyStr]:
        while True:
            line = await self.readline()
            if line:
                yield line
            else:
                break

    async def read(self, size: int = -1) -> AnyStr:
        return await asyncio.to_thread(self._io.read, size)

    async def readline(self) -> AnyStr:
        return await asyncio.to_thread(self._io.readline)

    async def readlines(self) -> list[AnyStr]:
        return await asyncio.to_thread(self._io.readlines)

    async def write(self, b: AnyStr, /) -> int:
        return await asyncio.to_thread(self._io.write, b)

    async def writelines(self, lines: Iterable[AnyStr], /) -> None:
        return await asyncio.to_thread(self._io.writelines, lines)


@dataclasses.dataclass(slots=True, eq=False)
class _AsyncPathIterator(AsyncIterator[Path]):
    iterator: Iterator[Path]

    async def __anext__(self) -> Path:
        path = await asyncio.to_thread(next, self.iterator, None)
        if path is None:
            raise StopAsyncIteration from None

        return path


stat = to_thread(Path.stat)
is_dir = to_thread(Path.is_dir)
is_file = to_thread(Path.is_file)
exists = to_thread(Path.exists)
unlink = remove = to_thread(Path.unlink)
mkdir = to_thread(Path.mkdir)
touch = to_thread(Path.touch)
read_text = to_thread(Path.read_text)
read_bytes = to_thread(Path.read_bytes)
write_bytes = to_thread(Path.write_bytes)
write_text = to_thread(Path.write_text)
resolve = to_thread(Path.resolve)
copy = to_thread(shutil.copy)
move = to_thread(shutil.move)
chmod = to_thread(Path.chmod)


def glob(path: Path, pattern: str) -> _AsyncPathIterator:
    return _AsyncPathIterator(path.glob(pattern))


def rglob(path: Path, pattern: str) -> _AsyncPathIterator:
    return _AsyncPathIterator(path.rglob(pattern))


@overload
def open(
    path: Path,
    mode: OpenBinaryMode,
    buffering: int = ...,
    encoding: str | None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
) -> AsyncIO[bytes]: ...


@overload
def open(
    path: Path,
    mode: OpenTextMode = ...,
    buffering: int = ...,
    encoding: str | None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
) -> AsyncIO[str]: ...


def open(
    path: Path,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
) -> AsyncIO[Any]:
    coro = asyncio.to_thread(path.open, mode, buffering, encoding, errors, newline)
    return AsyncIO(coro)


async def get_size(path: Path) -> int | None:
    """If path exists and is a file, returns its size. Returns `None` otherwise"""

    # Manually parse stat result to make sure we only use 1 fs call

    try:
        stat_result = await stat(path)
    except (OSError, ValueError):
        return
    else:
        if not S_ISREG(stat_result.st_mode):
            raise IsADirectoryError(path)
        return stat_result.st_size


@contextlib.asynccontextmanager
async def temp_dir() -> AsyncGenerator[Path]:
    temp_dir = await asyncio.to_thread(tempfile.TemporaryDirectory, prefix="cdl_", ignore_cleanup_errors=True)
    try:
        yield Path(temp_dir.name)
    finally:
        await asyncio.to_thread(temp_dir.cleanup)


class AsyncContextManagerMixin(ABC):
    __ctx: AbstractAsyncContextManager[object, bool | None] | None = None

    @final
    async def __aenter__(self) -> Self:
        if self.__ctx is not None:
            raise RuntimeError(f"{type(self).__qualname__} does not support re-entrance")

        ctx = self._asyncctx_()
        me = await ctx.__aenter__()
        self.__ctx = ctx
        return cast("Self", me)

    @final
    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> _ExitT_co:
        assert self.__ctx is not None
        ctx = self.__ctx
        del self.__ctx
        return cast("_ExitT_co", await ctx.__aexit__(exc_type, exc_val, exc_tb))

    @abstractmethod
    def _asyncctx_(self) -> AbstractAsyncContextManager[object, bool | None]: ...


def run(main: Coroutine[Any, Any, _T]) -> _T:
    def _loop_factory() -> asyncio.AbstractEventLoop:
        loop = asyncio.new_event_loop()
        if sys.version_info > (3, 12):
            loop.set_task_factory(asyncio.eager_task_factory)
        return loop

    with asyncio.Runner(loop_factory=_loop_factory) as runner:
        return runner.run(main)
