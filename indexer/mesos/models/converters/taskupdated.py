from datetime import datetime, timezone
from typing import Dict, Any
from uuid import uuid4

from indexer.mesos.models.converters.spec import MesosEventSourceSpecConverter
from indexer.mesos.models.converters.util import (
    get_appname,
    get_task_namespace,
    remove_task_namespace,
)
from indexer.mesos.models.spec import MesosEventSourceSpec
from indexer.mesos.models.taskupdated import MesosTaskUpdatedEvent
from indexer.models.converter import ModelConverter
from indexer.models.event import (
    Event,
    BackendInfoSpec,
    TaskInfoSpec,
    AgentInfoSpec,
    EventSourceSpec,
    ErrorSpec,
)
from indexer.models.util import get_backend_info


class MesosTaskUpdatedEventConverter(
    ModelConverter[Event, MesosTaskUpdatedEvent]
):
    @staticmethod
    def to_asgard_model(other: MesosTaskUpdatedEvent) -> Event:
        message = None
        extra: Dict[str, Any] = {}

        if other.status.reason:
            extra["error"] = ErrorSpec(
                message=other.status.message, reason=other.status.reason
            )
        elif other.status.message:
            extra["message"] = other.status.message

        task_id = other.status.task_id.value
        agent_id = other.status.agent_id.value
        return Event(
            id=str(uuid4()),
            date=datetime.fromtimestamp(other.status.timestamp)
            .astimezone(timezone.utc)
            .isoformat(),
            appname=get_appname(task_id),
            namespace=get_task_namespace(task_id),
            backend_info=BackendInfoSpec(name=get_backend_info(task_id)),
            task=TaskInfoSpec(id=remove_task_namespace(task_id)),
            agent=AgentInfoSpec(id=agent_id),
            status=other.status.state,
            source=MesosEventSourceSpecConverter.to_asgard_model(
                other.status.source
            ),
            **extra,
        )

    @staticmethod
    def to_client_model(other: Event) -> MesosTaskUpdatedEvent:
        raise NotImplementedError
