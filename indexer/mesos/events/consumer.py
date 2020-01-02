from typing import List

from aiohttp import ClientSession

from indexer.conf import settings
from indexer.consumer import Consumer


class EventConsumer(Consumer):
    def __init__(self, conn, *args, **kwargs) -> None:
        Consumer.__init__(self, conn, *args, **kwargs)

    async def consume(self):
        client = ClientSession()
        for url in self.conn.urls:
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
