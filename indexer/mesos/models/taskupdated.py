from pydantic import BaseModel

from indexer.mesos.models.spec import EventSourceSpec, TaskState, TaskStatusSpec


class MesosTaskUpdatedEvent(BaseModel):
    state: TaskState
    status: TaskStatusSpec
