import json
from typing import List

from aiohttp.web import Request, StreamResponse
from aioresponses import aioresponses
from asynctest import skip
from asyncworker import App, RouteTypes
from asyncworker.testing import HttpClientContext
from tests.base import BaseTestCase
from yarl import URL

from indexer.connection import HTTPConnection
from indexer.mesos.events.consumer import MesosEventConsumer

mesos_task_added_event_data = {
    "task_added": {
        "task": {
            "agent_id": {
                "value": "79ad3a13-b567-4273-ac8c-30378d35a439-S14522"
            },
            "container": {"type": "DOCKER"},
            "name": "sleep.sieve",
            "state": "TASK_STAGING",
            "task_id": {
                "value": "sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            },
        }
    },
    "type": "TASK_ADDED",
}


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
        mesos_base_url = "http://10.0.0.1:5050"
        with aioresponses() as rsps:
            rsps.post(f"{mesos_base_url}/api/v1", status=200)
            consumer = MesosEventConsumer(HTTPConnection(urls=[mesos_base_url]))
            await consumer.connect()
            self.assertEqual(1, len(rsps.requests.keys()))
            request_key = ("POST", URL(f"{mesos_base_url}/api/v1"))

            request_payload = rsps.requests[request_key][0].kwargs["json"]
            self.assertEqual(request_payload, {"type": "SUBSCRIBE"})

    async def test_parse_chunks_until_full_event_is_received(self):
        app = App()

        @app.route(["/api/v1"], RouteTypes.HTTP, methods=["POST"])
        async def api_v1(request: Request):
            event_str = json.dumps(mesos_task_added_event_data)
            len_data = len(event_str)
            resp = StreamResponse(status=200)
            await resp.prepare(request)
            await resp.write(f"{len_data}\n{event_str}".encode("utf-8"))
            return resp

        async with HttpClientContext(app) as client:
            url = f"http://{client._server.host}:{client._server.port}"
            consumer = MesosEventConsumer(HTTPConnection(urls=[url]))
            await consumer.connect()
            events = [ev async for ev in consumer.events()]
            self.assertEqual(1, len(events))
            self.assertEqual("sieve", events[0].namespace)
            self.assertEqual("sleep", events[0].appname)

    async def test_parse_raw_mesos_event_data(self):
        """
        Parseia bytes e retorna MesosEvent
        """
        event_data = json.dumps(mesos_task_added_event_data)
        len_data = len(event_data)
        record_io_raw_data = f"{len_data}\n{event_data}"

        consumer = MesosEventConsumer(HTTPConnection(urls=[""]))
        mesos_event = consumer._parse_recordio_event(
            record_io_raw_data.encode("utf-8")
        )
        self.assertEqual(mesos_event.type, mesos_task_added_event_data["type"])
