import asyncio
import json
from typing import AsyncGenerator, List, AsyncIterator, Coroutine, Any

from aiohttp import ClientSession
from aiologger.loggers.json import JsonLogger

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.consumer import Consumer
from indexer.mesos.models import MesosEvent
from indexer.mesos.models.converter import MesosEventConverter
from indexer.models.event import Event


class MesosEventConsumer(Consumer):
    def __init__(self, conn: HTTPConnection) -> None:
        Consumer.__init__(self, conn)

    async def connect(self) -> None:
        client = ClientSession()
        for url in self.conn.urls:
            resp = await client.post(
                f"{url}/api/v1", json={"type": "SUBSCRIBE"}
            )
            self.response = resp

    def _parse_recordio_event(self, data: bytes) -> MesosEvent:
        size, data_bytes = data.decode("utf-8").split("\n")
        mesos_event_data = json.loads(data_bytes)
        return MesosEvent(**mesos_event_data)

    async def events(self):
        _data = b""
        async for chunk, end in self.response.content.iter_chunks():
            _data += chunk
            if end:
                size, data_bytes = _data.decode("utf-8").split("\n")
                mesos_event_data = json.loads(data_bytes)
                _data = b""
                yield MesosEventConverter.to_asgard_model(
                    MesosEvent(**mesos_event_data)
                )
