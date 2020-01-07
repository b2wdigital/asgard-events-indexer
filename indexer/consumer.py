import asyncio
from abc import abstractmethod, ABC
from typing import AsyncGenerator, List

from aiohttp import ClientError

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.models.event import Event


class Consumer(ABC):
    def __init__(self, conn: HTTPConnection) -> None:
        self.conn = conn
        self._run = True

    @abstractmethod
    async def connect(self) -> None:
        """
        Connect no stream de eventos.
        Deve guardar internamente esse resultado para ser usado
        no self.events()
        """
        raise NotImplementedError

    @abstractmethod
    async def events(self) -> AsyncGenerator[Event, None]:
        """
        Generator Assincrono que retorna todos os eventos do
        stream de eventos sendo consumido
        """
        raise NotImplementedError

    async def write_output(self, events: List[Event]) -> None:
        raise NotImplementedError

    def should_run(self) -> bool:
        """
        Método para ajudar nos testes, para facilicar o teste de loops
        infinitos
        """
        logger.debug({"event": "should-run", "value": self._run})
        return self._run

    async def start(self):
        while self.should_run():
            try:
                await logger.debug({"event": "Connect"})
                await self.connect()
                async for event in self.events():
                    await self.write_output([event])
            except (ClientError, asyncio.TimeoutError) as e:
                await logger.exception(
                    {"event": "exception-consuming-events", "exc": str(e)}
                )
