from typing import Optional, Dict, List

from pydantic import BaseModel, validator


class MesosTaskDataConfigSpec(BaseModel):
    Hostname: str
    Image: str
    Labels: Dict[str, str]


class MesosTaskDataStateSpec(BaseModel):
    Running: bool
    Pid: int
    ExitCode: int
    Error: str


class MesosTaskDataHostConfigSpec(BaseModel):
    CpuShares: int
    CpuQuota: int
    MemorySwap: int
    MemorySwappiness: Optional[int]


class MesosTaskDataMountItemSpec(BaseModel):

    Type: str
    Source: str
    Destination: str
    Mode: str
    RW: bool


class MesosTaskDataSpec(BaseModel):
    Name: str
    config: MesosTaskDataConfigSpec
    State: MesosTaskDataStateSpec
    HostConfig: MesosTaskDataHostConfigSpec
    Mounts: List[MesosTaskDataMountItemSpec]

    @validator("Name")
    def remove_slashes(cls, v):
        return v.strip("/")
