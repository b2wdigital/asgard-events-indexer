from datetime import datetime, timezone
from enum import Enum, auto
from typing import Tuple
from uuid import uuid4

from indexer.mesos.models.event import MesosRawEvent
from indexer.mesos.models.taskadded import MesosTaskAddedEvent
from indexer.models.converter import ModelConverter
from indexer.models.event import (
    Event,
    BackendInfoSpec,
    TaskInfoSpec,
    AgentInfoSpec,
    TaskStatus,
)


class SplitType(Enum):
    MARATHON = auto()
    CHRONOS = auto()


def split_task_id(task_id: str, split_type: SplitType) -> Tuple[str, str, str]:
    """
    Recebe um task_id e retorna 3 valores: 
    (namespace, appname, task_id_sem_namespace)
    """
    if split_type == SplitType.MARATHON:
        namespace = task_id[: task_id.index("_")]
        task_id_no_namespace = task_id[task_id.index("_") + 1 :]
        appname = task_id_no_namespace.split(".")[0].replace("_", "/")
        return namespace, appname, task_id_no_namespace

    prefix, time, try_count, job_name, _ = task_id.split(":")
    job_name_parts = job_name.split("-")
    namespace = job_name_parts[0]
    appname = "-".join(job_name_parts[1:])
    # No caso do Chronos o task_name e appname são iguais
    return namespace, appname, appname


def get_task_namespace(task_id: str) -> str:
    split_type = (
        SplitType.CHRONOS if task_id.startswith("ct") else SplitType.MARATHON
    )

    namespace, _, _ = split_task_id(task_id, split_type)
    return namespace


def remove_task_namespace(task_id: str) -> str:

    split_type = (
        SplitType.CHRONOS if task_id.startswith("ct") else SplitType.MARATHON
    )

    _, _, task_name = split_task_id(task_id, split_type)
    return task_name


def get_appname(task_id: str) -> str:
    split_type = (
        SplitType.CHRONOS if task_id.startswith("ct") else SplitType.MARATHON
    )

    _, appname, _ = split_task_id(task_id, split_type)
    return appname


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
            backend_info=BackendInfoSpec(name="Mesos"),
            task=TaskInfoSpec(id=remove_task_namespace(task_id)),
            agent=AgentInfoSpec(id=agent_id),
            status=other.task.state,
        )
