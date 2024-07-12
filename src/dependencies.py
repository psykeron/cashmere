import inspect
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, ContextManager

from anyio import CapacityLimiter
from anyio.to_thread import run_sync


@dataclass
class Dependency:
    name: str
    dep: Any


@dataclass
class AsIsDependency(Dependency):
    """
    Whatever is in the contained dependency will be passed to the subscriber as-is.
    """

    pass


@dataclass
class FunctionDependency(Dependency):
    """
    The function will be run and the result will be passed to the subscriber.

    The only supported contents of the contained dependency are:

    1. A single async generator function, e.g.:
        ```
        def the_func():
            return "abc"
        ```
    """

    pass


@dataclass
class AsyncGeneratorDependency(Dependency):
    """
    The function will be run once and the result will be passed to the subscriber.
    The generator will then be run to close the generator.

    The only supported contents of the contained dependency are:

    1. A single async generator function, e.g.:
        ```
        async def the_func():
            async with a_func() as a:
                yield a
        ```

    2. A single async generator function with a try finally block, e.g.:
        ```
        async def the_func():
            try:
                a = await a_func()
                yield a
            finally:
                await a.close()
                await a.do_other_things()
        ```
    """

    pass


@dataclass
class GeneratorDependency(Dependency):
    """
    The function will be run once and the result will be passed to the subscriber.
    The generator will then be run to close the generator.

    The only supported contents of the contained dependency are:

    1. A single generator function, e.g.:
        ```
        def the_func():
            with a_func() as a:
                yield a
        ```

    2. A single generator function with a try finally block, e.g.:
        ```
        def the_func():
            try:
                a = a_func()
                yield a
            finally:
                a.close()
                a.do_other_things()
        ```
    """

    pass


DependencyTypes = (
    AsIsDependency | FunctionDependency | AsyncGeneratorDependency | GeneratorDependency
)


async def solve_dependency(dependency: DependencyTypes, stack: AsyncExitStack) -> Any:
    if isinstance(dependency, AsIsDependency):
        return dependency.dep

    if isinstance(dependency, FunctionDependency):
        return dependency.dep()

    if isinstance(dependency, GeneratorDependency):
        return await solve_generator(call=dependency.dep, stack=stack)

    if isinstance(dependency, AsyncGeneratorDependency):
        return await solve_generator(call=dependency.dep, stack=stack)


@asynccontextmanager
async def run_in_thread(cm: ContextManager) -> AsyncGenerator[Any, None]:
    exit_limiter = CapacityLimiter(1)
    try:
        yield await run_sync(cm.__enter__, limiter=exit_limiter)
    except Exception as e:
        ok = bool(await run_sync(cm.__exit__, type(e), e, None, limiter=exit_limiter))
        if not ok:
            raise e
    else:
        await run_sync(cm.__exit__, None, None, None, limiter=exit_limiter)


def is_async_gen_callable(call):
    if inspect.isasyncgenfunction(call):
        return True
    dunder_call = getattr(call, "__call__", None)  # noqa: B004
    return inspect.isasyncgenfunction(dunder_call)


def is_gen_callable(call):
    if inspect.isgeneratorfunction(call):
        return True
    dunder_call = getattr(call, "__call__", None)  # noqa: B004
    return inspect.isgeneratorfunction(dunder_call)


async def solve_generator(*, call: Callable[..., Any], stack: AsyncExitStack) -> Any:
    if is_gen_callable(call):
        cm = run_in_thread(contextmanager(call)())
    if is_async_gen_callable(call):
        cm = asynccontextmanager(call)()
    return await stack.enter_async_context(cm)
