import logging
import os
from typing import List

from aiologger.loggers.json import JsonLogger
from pydantic import BaseSettings


class Settings(BaseSettings):

    MESOS_MASTER_URLS: List[str]

    class Config:
        env_prefix = os.getenv("ENV", "INDEXER").upper() + "_"


settings = Settings()

logger = JsonLogger.with_default_handlers(flatten=True, level=logging.INFO)
