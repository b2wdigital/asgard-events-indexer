from typing import List

from aiohttp.web import StreamResponse, Request
from asynctest import skip
from asyncworker import App, RouteTypes
from asyncworker.testing import HttpClientContext
from tests.base import BaseTestCase

from indexer.connection import HTTPConnection
from indexer.mesos.events.consumer import MesosEventConsumer


class MesosConsumerTest(BaseTestCase):
    async def setUp(self):
        self.app = App()

        @self.app.route(["/api/v1"], RouteTypes.HTTP, methods=["POST"])
        async def api_v1(request: Request):
            resp = StreamResponse(status=200)
            await resp.prepare(request)
            await resp.write(b"OK")
            await resp.write(b"OK_Again")
            return resp

    async def test_start_calls_on_chunk_method(self):
        """
        Confere que o método `self.on_chunk()` é chamado
        para cada chunk recebido do response
        """

        async with HttpClientContext(self.app) as client:
            url = f"http://{client._server.host}:{client._server.port}"
            consumer = MesosEventConsumer(HTTPConnection(urls=[url]))

            await consumer.connect()
            chunks = list([c async for c in consumer.events()])

            self.assertEqual([b"OK", b"OK_Again"], chunks)

    async def test_calls_api_v1_with_correct_payload(self):
        """
        Precisamos fazer o POST /api/v1 passando um body
        {"type": "SUBSCRIBE"}
        """
        app = App()

        @app.route(["/api/v1"], type=RouteTypes.HTTP, methods=["POST"])
        async def index(request: Request):
            data = await request.text()
            resp = StreamResponse(status=200)
            await resp.prepare(request)
            await resp.write(data.encode("utf-8"))
            return resp

        async with HttpClientContext(app) as client:
            url = f"http://{client._server.host}:{client._server.port}"
            consumer = MesosEventConsumer(HTTPConnection(urls=[url]))
            await consumer.connect()

            chunks = list([x async for x in consumer.events()])

            self.assertEqual([b'{"type": "SUBSCRIBE"}'], chunks)

    @skip("to be implemented")
    async def test_reconnect_if_exception_on_current_connection(self):

        """
        Tentar fazer uma conection que entrega 2 chunks e quebra.
        Quando re-conecta entyra mais 3 chunks. Conferir que no final
        temos 5 chunks.
        """
        self.fail()

    @skip("to be implemented")
    async def test_calls_on_record_method(self):
        """
        Confere que quando recebemos um novo chunk (self.on_chunk())
        fazemos o parsing e chamamos o método self.on_record(...) passando
        o Record já parseado.
        """
        self.fail()

    @skip("to be implemented")
    async def test_calls_method_for_each_record_type(self):
        """
        Construimos o Record específico (TASK_ADDED, TASK_UPDATED, etc) e chamamos
        um método específico para processar esse Record.
        """
        self.fail()
