from datetime import datetime, timezone
from typing import List

from aioelasticsearch import Elasticsearch

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.models.event import Event


class OutputWritter:
    async def write(self, events: List[Event]) -> None:
        for e in events:
            await logger.info(e.dict())


class ElasticSearchOutputWritter(OutputWritter):
    def __init__(self, conn: HTTPConnection) -> None:
        self.conn = conn
        self.client = Elasticsearch(hosts=conn.urls)

    async def write(self, events: List[Event]) -> None:
        await self.client.index(
            self._get_index_name(), "event", events[0].dict()
        )

    def _get_index_name(self):
        date_part = datetime.utcnow()
        date_str = date_part.strftime("%Y-%m-%d-%H")
        return f"asgard-events-{date_str}"
