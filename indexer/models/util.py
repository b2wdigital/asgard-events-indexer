from indexer.models.event import BackendInfoTypes


def get_backend_info(task_id: str) -> BackendInfoTypes:
    """
    Dado o nome original de uma task, retorna qual backend a task pertence.
    """
    if task_id.startswith("ct"):
        return BackendInfoTypes.CHRONOS
    return BackendInfoTypes.MARATHON
