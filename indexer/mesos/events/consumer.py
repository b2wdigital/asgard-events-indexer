from aiohttp import ClientSession

from indexer.consumer import Consumer


class MesosEventConsumer(Consumer):
    def __init__(self, conn, *args, **kwargs) -> None:
        Consumer.__init__(self, conn, *args, **kwargs)

    async def connect(self):
        client = ClientSession()
        for url in self.conn.urls:
            resp = await client.post(
                f"{url}/api/v1", json={"type": "SUBSCRIBE"}
            )
            self.response = resp

    async def events(self):
        _data = b""
        async for chunk, end in self.response.content.iter_chunks():
            _data += chunk
            if end:
                yield _data
                _data = b""
