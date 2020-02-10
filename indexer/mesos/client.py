from typing import Dict, Any, List, Optional

from aiohttp.client import ClientSession, ClientError
from pydantic import BaseModel

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.mesos.models.spec import AgentIdSpec


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
