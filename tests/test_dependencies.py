from contextlib import AsyncExitStack

from src.dependencies import (
    AsIsDependency,
    AsyncGeneratorDependency,
    FunctionDependency,
    GeneratorDependency,
    solve_dependency,
)


class TestSolveDependency:
    async def test_solve_dependency_with_asis_dependency_returns_dependency(self):
        dependency = AsIsDependency(name="a_dep", dep="dependency")
        result = await solve_dependency(dependency, AsyncExitStack())
        assert result == "dependency"

    async def test_solve_dependency_with_function_dependency_returns_dependency_result(
        self,
    ):
        dependency = FunctionDependency(name="a_dep", dep=lambda: "dependency_result")
        result = await solve_dependency(dependency, AsyncExitStack())
        assert result == "dependency_result"

    async def test_solve_dependency_with_generator_dependency_returns_generator_result(
        self,
    ):
        def generator():
            yield "generator_result"

        dependency = GeneratorDependency(name="a_dep", dep=generator)
        result = await solve_dependency(dependency, AsyncExitStack())
        assert result == "generator_result"

    async def test_solve_dependency_with_async_generator_dependency_returns_generator_result(
        self,
    ):
        async def async_generator():
            yield "async_generator_result"

        dependency = AsyncGeneratorDependency(name="a_dep", dep=async_generator)
        result = await solve_dependency(dependency, AsyncExitStack())
        assert result == "async_generator_result"
