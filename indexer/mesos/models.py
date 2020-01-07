from typing import Optional

from pydantic import BaseModel


class AgentIdSpec(BaseModel):
    value: str


class TaskSpec(BaseModel):
    agend_id: AgentIdSpec


class TaskAddedSpec(BaseModel):
    task: TaskSpec


class MesosEvent(BaseModel):
    type: str
    task_added: Optional[TaskAddedSpec]
