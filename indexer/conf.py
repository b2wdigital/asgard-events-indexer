import logging
import os
from typing import List

from aiologger.loggers.json import JsonLogger
from pydantic import BaseSettings


class Settings(BaseSettings):

    MESOS_MASTER_URLS: List[str]
    ES_OUTPUT_URLS: List[str]
    OUTPUT_TO_STDOUT: bool = False
    TASK_FILE_CONTENT_LENGTH: int = 4096

    class Config:
        env_prefix = os.getenv("ENV", "INDEXER").upper() + "_"


settings = Settings()

logger = JsonLogger.with_default_handlers(flatten=True, level=logging.INFO)
