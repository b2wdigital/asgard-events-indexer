import os
from typing import List

from pydantic import BaseSettings


class Settings(BaseSettings):

    MESOS_MASTER_URLS: List[str]

    class Config:
        env_prefix = os.getenv("ENV", "INDEXER").upper() + "_"


settings = Settings()
