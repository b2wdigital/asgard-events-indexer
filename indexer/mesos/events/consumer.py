import asyncio
import json
from typing import Dict, Any, AsyncGenerator, List, AsyncIterator, Coroutine

from aiohttp import ClientSession
from aiologger.loggers.json import JsonLogger

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.consumer import Consumer
from indexer.mesos.events import MesosEvents
from indexer.mesos.models import MesosRawEvent
from indexer.mesos.models.converter import MesosTaskAddedEventConverter
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

    def _parse_recordio_event(self, data: bytes) -> MesosRawEvent:
        size, data_bytes = data.decode("utf-8").split("\n")
        mesos_event_data = json.loads(data_bytes)
        return MesosRawEvent(**mesos_event_data)

    async def events(self):
        async for mesos_event_data in self._json_chunks():
            if mesos_event_data.type == MesosEvents.TASK_ADDED:
                yield MesosTaskAddedEventConverter.to_asgard_model(
                    mesos_event_data.task_added
                )

    async def _json_chunks(self) -> AsyncGenerator[MesosRawEvent, None]:
        _data = b""
        async for chunk, end in self.response.content.iter_chunks():
            _data += chunk
            if end:
                size, data_bytes = _data.decode("utf-8").split("\n")
                mesos_event_data = json.loads(data_bytes)
                _data = b""
                yield MesosRawEvent(**mesos_event_data)

