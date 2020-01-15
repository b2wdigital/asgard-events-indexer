import json
from base64 import b64decode
from enum import Enum
from typing import Optional

from pydantic import BaseModel

from indexer.mesos.models.spec.taskdata import MesosTaskDataSpec


class AgentIdSpec(BaseModel):
    value: str


class ContainerSpec(BaseModel):
    type: str


class TaskIdSpec(BaseModel):
    value: str


class TaskState(str, Enum):
    TASK_STAGING = "TASK_STAGING"
    TASK_STARTING = "TASK_STARTING"
    TASK_RUNNING = "TASK_RUNNING"
    TASK_KILLING = "TASK_KILLING"
    TASK_LOST = "TASK_LOST"
    TASK_UNREACHABLE = "TASK_UNREACHABLE"
    TASK_FINISHED = "TASK_FINISHED"
    TASK_FAILED = "TASK_FAILED"
    TASK_KILLED = "TASK_KILLED"
    TASK_ERROR = "TASK_ERROR"
    TASK_DROPPED = "TASK_DROPPED"
    TASK_GONE = "TASK_GONE"
    TASK_GONE_BY_OPERATOR = "TASK_GONE_BY_OPERATOR"
    TASK_UNKNOWN = "TASK_UNKNOWN"


class TaskSpec(BaseModel):
    agent_id: AgentIdSpec
    container: ContainerSpec
    name: str
    state: TaskState
    task_id: TaskIdSpec


class MesosEventSourceSpec(str, Enum):
    SOURCE_EXECUTOR = "SOURCE_EXECUTOR"
    SOURCE_MASTER = "SOURCE_MASTER"
    SOURCE_AGENT = "SOURCE_AGENT"


class TaskStatusSpec(BaseModel):
    agent_id: AgentIdSpec
    message: Optional[str]
    reason: Optional[str]
    data: Optional[str]
    source: MesosEventSourceSpec
    state: TaskState
    task_id: TaskIdSpec
    timestamp: int

    def task_details(self) -> Optional[MesosTaskDataSpec]:
        if self.data:
            b64_decoded = b64decode(self.data)
            data_dict = json.loads(b64_decoded)
            task_data = data_dict[0]
            # Temos que mudar de Config pata config pois o pydantic
            # usa o nome Config para guardar as configuracões de um model
            # https://pydantic-docs.helpmanual.io/usage/model_config/
            task_data["config"] = task_data["Config"]
            del task_data["Config"]
            return MesosTaskDataSpec(**task_data)
        return None
