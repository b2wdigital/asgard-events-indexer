from indexer.models.event import BackendInfoTypes


def get_backend_info(task_id: str) -> BackendInfoTypes:
    if task_id.startswith("ct"):
        return BackendInfoTypes.CHRONOS
    return BackendInfoTypes.MARATHON
