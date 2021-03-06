from datetime import datetime, timezone
from uuid import uuid4

from indexer.mesos.models.converters.util import (
    get_appname,
    get_task_namespace,
    remove_task_namespace,
)
from indexer.mesos.models.taskadded import MesosTaskAddedEvent
from indexer.models.converter import ModelConverter
from indexer.models.event import (
    EventSourceSpec,
    Event,
    BackendInfoSpec,
    TaskInfoSpec,
    AgentInfoSpec,
)
from indexer.models.util import get_backend_info


class MesosTaskAddedEventConverter(ModelConverter[Event, MesosTaskAddedEvent]):
    @staticmethod
    def to_client_model(other: Event) -> MesosTaskAddedEvent:
        raise NotImplementedError

    @staticmethod
    def to_asgard_model(other: MesosTaskAddedEvent) -> Event:
        task_id = other.task.task_id.value
        agent_id = other.task.agent_id.value
        return Event(
            id=str(uuid4()),
            date=datetime.now(timezone.utc).isoformat(),
            appname=get_appname(task_id),
            namespace=get_task_namespace(task_id),
            backend_info=BackendInfoSpec(name=get_backend_info(task_id)),
            task=TaskInfoSpec(id=remove_task_namespace(task_id)),
            agent=AgentInfoSpec(id=agent_id),
            status=other.task.state,
            source=EventSourceSpec.SOURCE_MASTER,
        )
