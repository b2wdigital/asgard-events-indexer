import json
from typing import List

from aiohttp.web import Request, StreamResponse
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
            event_data = {"type": "SUBSCRIBED"}
            event_data_str = json.dumps(event_data)
            event_data_len = len(event_data_str)

            resp = StreamResponse(status=200)
            await resp.prepare(request)
            await resp.write(
                f"{event_data_len}\n{event_data_str}".encode("utf-8")
            )
            await resp.write(
                f"{event_data_len}\n{event_data_str}".encode("utf-8")
            )
            return resp

    async def test_calls_api_v1_with_correct_payload(self):
        """
        Precisamos fazer o POST /api/v1 passando um body
        {"type": "SUBSCRIBE"}
        """
        app = App()

        @app.route(["/api/v1"], type=RouteTypes.HTTP, methods=["POST"])
        async def index(request: Request):
            event_data_str = await request.text()
            event_data_len = len(event_data_str)
            resp = StreamResponse(status=200)
            await resp.prepare(request)
            await resp.write(
                f"{event_data_len}\n{event_data_str}".encode("utf-8")
            )
            return resp

        async with HttpClientContext(app) as client:
            url = f"http://{client._server.host}:{client._server.port}"
            consumer = MesosEventConsumer(HTTPConnection(urls=[url]))
            await consumer.connect()

            events = list([x async for x in consumer.events()])

            self.assertEqual(1, len(events))
            self.assertEqual("SUBSCRIBE", events[0].type)

    async def test_parse_chunks_until_full_event_is_received(self):
        """
        Vamos entregar um evento dividido em múltiplos chunks e o self.events()
        deve retornar corretamente *um* evento.
        """
        app = App()

        @app.route(["/api/v1"], RouteTypes.HTTP, methods=["POST"])
        async def api_v1(request: Request):
            event_data = '{"type": "TASK_ADDED", "task_added": {"task": {"agend_id": {"value": "79ad3a13-b567-4273-ac8c-30378d35a439-S14522"}}}}'
            len_data = len(event_data)
            resp = StreamResponse(status=200)
            await resp.prepare(request)
            await resp.write(f"{len_data}\n{event_data}".encode("utf-8"))
            return resp

        async with HttpClientContext(app) as client:
            url = f"http://{client._server.host}:{client._server.port}"
            consumer = MesosEventConsumer(HTTPConnection(urls=[url]))
            await consumer.connect()
            events = [ev async for ev in consumer.events()]
            self.assertEqual(1, len(events))
            self.assertEqual("TASK_ADDED", events[0].type)

    async def test_parse_raw_mesos_event_data(self):
        """
        Parseia bytes e retorna MesosEvent
        """
        event_dict = {
            "type": "TASK_ADDED",
            "task_added": {
                "task": {
                    "agend_id": {
                        "value": "79ad3a13-b567-4273-ac8c-30378d35a439-S14522"
                    }
                }
            },
        }
        event_data = json.dumps(event_dict)
        len_data = len(event_data)
        record_io_raw_data = f"{len_data}\n{event_data}"

        consumer = MesosEventConsumer(HTTPConnection(urls=[""]))
        mesos_event = consumer._parse_recordio_event(
            record_io_raw_data.encode("utf-8")
        )
        self.assertEqual(event_dict, mesos_event.dict())
