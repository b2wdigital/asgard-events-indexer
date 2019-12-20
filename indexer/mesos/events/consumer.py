from typing import List

from aiohttp import ClientSession
from pydantic import BaseModel

from indexer.conf import settings


class EventConsumer(BaseModel):
    async def startup(self):
        client = ClientSession()
        for url in settings.MESOS_MASTER_URLS:
            resp = await client.post(
                f"{url}/api/v1", json={"type": "SUBSCRIBE"}
            )
            _data = b""
            async for chunk, end in resp.content.iter_chunks():
                _data += chunk
                if end:
                    await self.on_chunk(_data)
                    _data = b""

    async def on_chunk(self, chunk: bytes) -> None:
        raise NotImplementedError

    async def shutdown(self):
        pass  # pragma: no cover
