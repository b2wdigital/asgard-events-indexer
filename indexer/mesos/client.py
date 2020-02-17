from typing import Dict, Any, List, Optional

from aiohttp.client import ClientSession, ClientError
from pydantic import BaseModel

from indexer.conf import settings, logger
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
    completed_frameworks: List[FrameworksInfoSpec]


class CompletedTaskInfo(BaseModel):
    id: str
    directory: str


class TaskOutputData(BaseModel):
    task: TaskIdSpec
    stdout: Optional[str]
    stderr: Optional[str]


class TaskFileListItem(BaseModel):
    size: int
    path: str


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
        for fwk in agent_state.frameworks + agent_state.completed_frameworks:
            for completed_executor in fwk.completed_executors:
                if completed_executor.id == task_id.value:
                    return completed_executor
        return None

    async def get_task_output_data(
        self, agent_addr: str, task_info: CompletedTaskInfo
    ) -> TaskOutputData:

        stdout_data = await self._get_task_file_content(
            agent_addr, task_info, "stdout"
        )

        stderr_data = await self._get_task_file_content(
            agent_addr, task_info, "stderr"
        )
        return TaskOutputData(
            task=TaskIdSpec(value=task_info.id),
            stdout=stdout_data,
            stderr=stderr_data,
        )

    async def _get_task_file_size(
        self, agent_addr: str, task_info: CompletedTaskInfo, file_name: str
    ) -> int:
        resp = await self.http.get(
            f"{agent_addr}/files/browse?path={task_info.directory}"
        )
        task_files_data = await resp.json()
        for task_item in task_files_data:
            if task_item["path"].endswith(file_name):
                return task_item["size"]

        return 0

    async def _get_task_file_content(
        self, agent_addr: str, task_info: CompletedTaskInfo, file_name: str
    ):
        file_size = await self._get_task_file_size(
            agent_addr, task_info, file_name
        )

        offset = max(0, file_size - settings.TASK_FILE_CONTENT_LENGTH)
        length = settings.TASK_FILE_CONTENT_LENGTH

        file_path = f"{task_info.directory}/{file_name}"

        resp = await self.http.get(
            f"{agent_addr}/files/read?path={file_path}&length={length}&offset={offset}"
        )
        data = await resp.json()
        return data["data"]
