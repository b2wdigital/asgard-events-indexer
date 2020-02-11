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
from indexer.models.event import BackendInfoTypes

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

                    if (
                        mesos_event_data["type"] == "TASK_UPDATED"
                        and mesos_event_data["task_updated"]["status"]["state"]
                        == "TASK_FINISHED"
                    ):
                        await logger.info(mesos_event_data)
                    yield MesosEvent(**mesos_event_data)
                except ValidationError:
                    await logger.exception(
                        {
                            "event": "unsoported-mesos-event-received",
                            "event-type": mesos_event_data.get("type"),
                        }
                    )


# async def pre_process_event(self, events: List[Event]) -> None:
#     # Call mesos API do get slave IP (using slave id)
#     # Se o slave não existir, lançamos uma exception e abortamos
#     # Call slave API do get Task info
#     # Download Stdout/Stderr
#     # Save to Google storage
#     # Update Document (?) Temos o ID do documento aqui?

#     # Abordagem
#     # Pega do Event o executor_id (bruto, do jeito que o Mesos entregou)
#     # Usa esse executor_id para iterar no state do slave.
#     # Dentro do executor_info (no /state) tem o campo `directory`
#     # Com esse campo directory, podemos voltar no slave e ir em /files/read?path=<directory>
# agent_addr = mesos_client.get_
#     pass
