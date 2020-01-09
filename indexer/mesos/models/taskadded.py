from pydantic import BaseModel

from indexer.mesos.models.spec import TaskSpec


class MesosTaskAddedEvent(BaseModel):
    task: TaskSpec
