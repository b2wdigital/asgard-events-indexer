from typing import List

from pydantic import BaseModel


class HTTPConnection(BaseModel):
    urls: List[str]
