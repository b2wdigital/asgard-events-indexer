from indexer.connection import HTTPConnection


class Consumer:
    def __init__(self, conn: HTTPConnection) -> None:
        self.conn = conn

    async def consume(self):
        raise NotImplementedError

    async def events(self):
        raise NotImplementedError
