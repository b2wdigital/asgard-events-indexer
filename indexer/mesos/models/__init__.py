from enum import Enum
from typing import Optional

from pydantic import BaseModel


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


class TaskAddedSpec(BaseModel):
    task: TaskSpec


class MesosEvent(BaseModel):
    type: str
    task_added: Optional[TaskAddedSpec]
