from typing import List

from indexer.conf import logger
from indexer.models.event import Event


class OutputWritter:
    async def write(self, events: List[Event]) -> None:
        for e in events:
            await logger.info(e.dict())


class ElasticSearchOutputWritter(OutputWritter):
    async def write(self, events: List[Event]) -> None:
        pass
