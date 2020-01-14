from enum import Enum, auto
from typing import Tuple

from indexer.models.event import BackendInfoTypes


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
