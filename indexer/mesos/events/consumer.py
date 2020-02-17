import json
from typing import List, AsyncGenerator, Optional

from aiohttp import ClientSession
from aiohttp.client import ClientTimeout
from pydantic import ValidationError

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.consumer import Consumer
from indexer.mesos.client import MesosClient
from indexer.mesos.models.converters.taskadded import (
    MesosTaskAddedEventConverter,
)
from indexer.mesos.models.converters.taskupdated import (
    MesosTaskUpdatedEventConverter,
)
from indexer.mesos.models.event import MesosEventTypes, MesosEvent
from indexer.mesos.models.spec import AgentIdSpec, TaskIdSpec
from indexer.models.event import Event, BackendInfoTypes
from indexer.models.util import BackendInfoTypes, get_backend_info

timeout_config = ClientTimeout(connect=2.0, sock_read=90.0)


class MesosEventConsumer(Consumer):

    http_client: ClientSession
    mesos_client: MesosClient

    def __init__(self, conn: HTTPConnection) -> None:
        Consumer.__init__(self, conn)

    async def connect(self) -> None:
        self.http_client = ClientSession(timeout=timeout_config)
        self.mesos_client = MesosClient(self.http_client, self.conn)
        for url in self.conn.urls:
            resp = await self.http_client.post(
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
                except ValidationError:
                    await logger.exception(
                        {
                            "event": "unsoported-mesos-event-received",
                            "event-type": mesos_event_data.get("type"),
                        }
                    )

    def _get_task_id_with_namespace(self, event):
        task_id_with_namespace = ""
        if get_backend_info(event.task.id) == BackendInfoTypes.CHRONOS:
            prefix, time, try_, appname = event.task.id.split(":")
            task_id_with_namespace = ":".join(
                [prefix, time, try_, f"{event.namespace}-{appname}", ""]
            )
        elif get_backend_info(event.task.id) == BackendInfoTypes.MARATHON:
            task_id_with_namespace = f"{event.namespace}_{event.task.id}"

        return task_id_with_namespace

    async def pre_process_event(self, events: List[Event]) -> None:
        for event in events:
            task_id = self._get_task_id_with_namespace(event)
            agent_addr = await self.mesos_client.get_agent_address(
                AgentIdSpec(value=event.agent.id)
            )
            task_info = await self.mesos_client.get_task_info(
                agent_addr, TaskIdSpec(value=task_id)
            )
            if task_info:
                output_data = await self.mesos_client.get_task_output_data(
                    agent_addr, task_info
                )
                event.task.stdout = output_data.stdout
                event.task.stderr = output_data.stderr
