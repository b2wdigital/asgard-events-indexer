from enum import Enum
from typing import Optional

from pydantic import BaseModel

from indexer.mesos.models.taskadded import MesosTaskAddedEvent


class MesosEventTypes(str, Enum):
    TASK_ADDED = "TASK_ADDED"


class MesosRawEvent(BaseModel):
    type: MesosEventTypes
    task_added: Optional[MesosTaskAddedEvent]
