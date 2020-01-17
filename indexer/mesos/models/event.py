from enum import Enum
from typing import Optional

from pydantic import BaseModel

from indexer.mesos.models.spec import MesosTaskDataSpec
from indexer.mesos.models.taskadded import MesosTaskAddedEvent
from indexer.mesos.models.taskupdated import MesosTaskUpdatedEvent


class MesosEventTypes(str, Enum):
    TASK_ADDED = "TASK_ADDED"
    TASK_UPDATED = "TASK_UPDATED"
    SUBSCRIBED = "SUBSCRIBED"


class MesosEvent(BaseModel):
    type: MesosEventTypes
    task_added: Optional[MesosTaskAddedEvent]
    task_updated: Optional[MesosTaskUpdatedEvent]

    def task_details(self) -> Optional[MesosTaskDataSpec]:
        if self.task_updated:
            return self.task_updated.status.task_details()
        return None
