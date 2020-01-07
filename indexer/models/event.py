from pydantic import BaseModel


class Event(BaseModel):
    id: str
    appname: str
