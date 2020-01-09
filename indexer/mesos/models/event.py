from typing import Optional

from pydantic import BaseModel

from indexer.mesos.events import MesosEvents
from indexer.mesos.models.taskadded import MesosTaskAddedEvent


class MesosRawEvent(BaseModel):
    type: MesosEvents
    task_added: Optional[MesosTaskAddedEvent]
