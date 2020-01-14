import json
from typing import AsyncGenerator, List, Optional

from aiohttp import ClientSession
from pydantic import ValidationError

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.consumer import Consumer
from indexer.mesos.models.converters.taskadded import (
    MesosTaskAddedEventConverter,
)
from indexer.mesos.models.converters.taskupdated import (
    MesosTaskUpdatedEventConverter,
)
from indexer.mesos.models.event import MesosEventTypes, MesosEvent
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
        async for mesos_event_data in self._mesos_events():
            if mesos_event_data.type == MesosEventTypes.TASK_ADDED:
                yield MesosTaskAddedEventConverter.to_asgard_model(
                    mesos_event_data.task_added
                )
            if mesos_event_data.type == MesosEventTypes.TASK_UPDATED:
                yield MesosTaskUpdatedEventConverter.to_asgard_model(
                    mesos_event_data.task_updated
                )

    async def _mesos_events(self) -> AsyncGenerator[Optional[MesosEvent], None]:
        _data = b""
        async for chunk, end in self.response.content.iter_chunks():
            _data += chunk
            if end:
                size, data_bytes = _data.decode("utf-8").split("\n")
                mesos_event_data = json.loads(data_bytes)
                _data = b""
                try:
                    yield MesosEvent(**mesos_event_data)
                except ValidationError as v:
                    await logger.exception(
                        {
                            "event": "unsoported-mesos-event-received",
                            "event-type": mesos_event_data.get("type"),
                        }
                    )
