from typing import Dict, Any, List, Optional

from aiohttp.client import ClientSession, ClientError
from pydantic import BaseModel

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.mesos.models.spec import AgentIdSpec, TaskIdSpec


class AgentNotFoundException(Exception):
    pass


class NoMoreMesosServersException(Exception):
    pass


class SlaveInfo(BaseModel):
    id: str
    hostname: str
    port: int


class SlaveAPIEndPointResponse(BaseModel):
    slaves: Optional[List[SlaveInfo]]


class CompletedExecutorSpec(BaseModel):
    id: str
    directory: str


class FrameworksInfoSpec(BaseModel):
    completed_executors: List[CompletedExecutorSpec]


class StateAPIEndPointResponse(BaseModel):
    frameworks: List[FrameworksInfoSpec]


class CompletedTaskInfo(BaseModel):
    id: str
    directory: str


class MesosClient:

    http: ClientSession

    def __init__(
        self, http_client: ClientSession, conn: HTTPConnection
    ) -> None:
        self.http = http_client
        self.conn = conn

    async def _get_json_data(self, path: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        for url in self.conn.urls:
            try:
                resp = await self.http.get(f"{url}/{path}")
                data = await resp.json()
                return data
            except ClientError as e:
                logger.exception(
                    {
                        "event": "client-error",
                        "exc": str(e),
                        "mesos-address": url,
                    }
                )
        else:
            raise NoMoreMesosServersException("servers")

    async def get_agent_address(self, agent_id: AgentIdSpec) -> str:
        data = await self._get_json_data(f"slaves?slave_id={agent_id.value}")
        slave_api_response = SlaveAPIEndPointResponse(**data)
        if slave_api_response.slaves:
            agent_info = slave_api_response.slaves[0]
            return f"http://{agent_info.hostname}:{agent_info.port}"
        raise AgentNotFoundException(f"Agent not found: id={agent_id.value}")

    async def get_task_info(
        self, agent_addr: str, task_id: TaskIdSpec
    ) -> Optional[CompletedTaskInfo]:
        """
        Dado um slave e uma task, retorna o info que representa essa task.
        Esse task_id é o task_id bruto incluindo o namespace, se existir.

        Retorna um objeto que contém a pasta onde estão o stdout/stderr dessa task.
        Esse path pode ser passado diretamente para a API do Agent (/files/read?path=<path>)
        """
        state = await self.http.get(f"{agent_addr}/state")
        agent_state = StateAPIEndPointResponse(**await state.json())

        completed_task_info = self._find_task_in_state(agent_state, task_id)
        if completed_task_info:
            return CompletedTaskInfo(**completed_task_info.dict())
        return None

    def _find_task_in_state(
        self, agent_state: StateAPIEndPointResponse, task_id: TaskIdSpec
    ) -> Optional[CompletedExecutorSpec]:
        for fwk in agent_state.frameworks:
            for completed_executor in fwk.completed_executors:
                if completed_executor.id == task_id.value:
                    return completed_executor
        return None
