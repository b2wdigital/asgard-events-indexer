import asyncio
from abc import abstractmethod, ABC
from typing import List

from aiohttp import ClientError

from indexer.conf import settings, logger
from indexer.connection import HTTPConnection
from indexer.models.event import Event
from indexer.writter import OutputWritter, ElasticSearchOutputWritter


class Consumer(ABC):
    def __init__(self, conn: HTTPConnection) -> None:
        self.conn = conn
        self._run = True
        self.output: List[OutputWritter] = []
        if settings.OUTPUT_TO_STDOUT:
            self.output.append(OutputWritter())
        if settings.ES_OUTPUT_URLS:
            self.output.append(ElasticSearchOutputWritter())

    @abstractmethod
    async def connect(self) -> None:
        """
        Connect no stream de eventos.
        Deve guardar internamente esse resultado para ser usado
        no self.events()
        """
        raise NotImplementedError

    @abstractmethod
    async def events(self):
        """
        Generator Assincrono que retorna todos os eventos do
        stream de eventos sendo consumido
        """
        raise NotImplementedError

    async def write_output(self, events: List[Event]) -> None:
        for out in self.output:
            await out.write(events)

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
