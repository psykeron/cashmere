import asyncio
from typing import List

from loguru import logger

from .app import Cashmere
from .exception import one_line_error


class CashmereCollection:
    """
    A collection of cashmere apps.
    It has the same interface as a Cashmere app.
    The namespace of the cashmere collection overwrites the namespace of the cashmere apps.
    """

    def __init__(
        self, namespace_prefix: str | None, apps: List[Cashmere] | None = None
    ) -> None:
        self.namespace_prefix = namespace_prefix
        self.apps = apps or []

        for app in self.apps:
            app.namespace_prefix = namespace_prefix

    async def thread_the_loom(self) -> None:
        logger.info("Threading the loom")
        for app in self.apps:
            logger.info(f"Threading the loom for: < app: {app.domain} />")
            await app.thread_the_loom()

    async def weave(self, fail_app_after_x_exceptions: int = 5) -> None:
        logger.info(f"Weaving cashmere with: {[app.domain for app in self.apps]}")

        exception_count = 0

        while exception_count < fail_app_after_x_exceptions:
            try:
                await asyncio.gather(*[app.weave() for app in self.apps])
            except Exception as e:
                exception_count += 1
                logger.error(
                    f"Error in cashmere collection:"
                    f" < exception_count: {exception_count} />"
                    f" < error: {one_line_error(e)} />."
                )
