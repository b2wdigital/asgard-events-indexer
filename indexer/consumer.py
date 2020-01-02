from abc import abstractmethod

from indexer.connection import HTTPConnection


class Consumer:
    def __init__(self, conn: HTTPConnection) -> None:
        self.conn = conn

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
