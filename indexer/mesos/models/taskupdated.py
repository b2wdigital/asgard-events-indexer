from pydantic import BaseModel

from indexer.mesos.models.spec import TaskState, TaskStatusSpec


class MesosTaskUpdatedEvent(BaseModel):
    state: TaskState
    status: TaskStatusSpec
