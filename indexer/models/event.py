from enum import Enum
from typing import Optional

from pydantic import BaseModel


class EventSourceSpec(str, Enum):
    SOURCE_MASTER = "MASTER"
    SOURCE_EXECUTOR = "EXECUTOR"
    SOURCE_AGENT = "AGENT"


class BackendInfoTypes(str, Enum):
    MARATHON = "Mesos/Marathon"
    CHRONOS = "Mesos/Chronos"


class BackendInfoSpec(BaseModel):
    name: str


class TaskInfoSpec(BaseModel):
    id: str


class AgentInfoSpec(BaseModel):
    id: str


class TaskStatus(str, Enum):
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


class ErrorSpec(BaseModel):
    message: str
    reason: str


class Event(BaseModel):
    id: str
    date: str
    appname: str
    namespace: str
    source: EventSourceSpec = EventSourceSpec.SOURCE_MASTER
    backend_info: BackendInfoSpec
    task: TaskInfoSpec
    agent: AgentInfoSpec
    status: TaskStatus
    error: Optional[ErrorSpec]
