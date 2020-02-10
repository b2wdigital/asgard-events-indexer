from pydantic import BaseModel

from indexer.mesos.models.spec import TaskState, TaskStatusSpec, FrameworkIdSpec


class MesosTaskUpdatedEvent(BaseModel):
    state: TaskState
    status: TaskStatusSpec
    framework_id: FrameworkIdSpec
