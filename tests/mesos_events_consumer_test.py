import json

from aiohttp.client import ClientTimeout
from aiohttp.web import Request, StreamResponse
from aioresponses import aioresponses
from asynctest import mock
from asynctest.mock import CoroutineMock, ANY
from asyncworker import App, RouteTypes
from asyncworker.testing import HttpClientContext
from yarl import URL

from indexer.conf import settings
from indexer.connection import HTTPConnection
from indexer.mesos.events import consumer as mesos_consumer_module
from indexer.mesos.events.consumer import MesosEventConsumer, timeout_config
from indexer.models.event import BackendInfoTypes, EventSourceSpec
from tests.base import LOGGER_MOCK, BaseTestCase

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

mesos_state_finished_event_data = {
    "task_updated": {
        "framework_id": {"value": "4783cf15-4fb1-4c75-90fe-44eeec5258a7-0001"},
        "state": "TASK_FINISHED",
        "status": {
            "executor_id": {"value": "ct:1581355920078:0:asgard-heimdall:"},
            "agent_id": {"value": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"},
            "message": "Container exited with status 0",
            "source": "SOURCE_EXECUTOR",
            "state": "TASK_FINISHED",
            "task_id": {"value": "ct:1578593280013:0:asgard-heimdall:"},
            "timestamp": 1_578_685_955,
        },
    },
    "type": "TASK_UPDATED",
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

    async def test_parse_unsupported_event_type(self):
        """
        Devemos falhar de forma consciente se recebermos um evento
        que não estamos preparados para tratar, ou seja:
        - Percebemos o erro
        - Lançamos exception avisando do problema
        - Continuamos a processar os próximos eventos
        """
        app = App()

        UNKNOWN_EVENT_TYPE = "DONT_KNOW_THIS_EVENT"

        unknown_event_data = {
            "type": UNKNOWN_EVENT_TYPE,
            "data": {"key": "value"},
        }
        unknown_event_str = json.dumps(unknown_event_data)
        unknown_event_len = len(unknown_event_str)

        @app.route(["/api/v1"], RouteTypes.HTTP, methods=["POST"])
        async def api_v1(request: Request):
            event_str = json.dumps(mesos_task_added_event_data)
            len_data = len(event_str)
            resp = StreamResponse(status=200)
            await resp.prepare(request)
            await resp.write(f"{len_data}\n{event_str}".encode("utf-8"))
            await resp.write(f"{len_data}\n{event_str}".encode("utf-8"))
            await resp.write(
                f"{unknown_event_len}\n{unknown_event_str}".encode("utf-8")
            )
            return resp

        from unittest import mock

        with mock.patch(
            "indexer.mesos.events.consumer.logger", LOGGER_MOCK
        ) as mock_logger:
            async with HttpClientContext(app) as client:
                url = f"http://{client._server.host}:{client._server.port}"
                consumer = MesosEventConsumer(HTTPConnection(urls=[url]))
                await consumer.connect()
                events = [ev async for ev in consumer.events()]
                self.assertEqual(2, len(events))
                self.assertEqual("sieve", events[0].namespace)
                self.assertEqual("sleep", events[0].appname)
                self.assertEqual("sieve", events[1].namespace)
                self.assertEqual("sleep", events[1].appname)
                mock_logger.exception.assert_awaited_with(
                    {
                        "event": "unsoported-mesos-event-received",
                        "event-type": UNKNOWN_EVENT_TYPE,
                    }
                )

    async def test_parse_task_updated_event_state_task_finished(self):
        """
        type: TASK_UPATED
        state: TASK_FINISHED
        Pegar também a mensagem que diz o valor de retorno do container
        """

        asgard_event_expected_data = {
            "id": ANY,
            "date": "2020-01-10T19:52:35+00:00",
            "appname": "heimdall",
            "namespace": "asgard",
            "backend_info": {"name": BackendInfoTypes.CHRONOS},
            "task": {"id": "ct:1578593280013:0:heimdall"},
            "agent": {"id": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"},
            "status": "TASK_FINISHED",
            "source": EventSourceSpec.SOURCE_EXECUTOR,
            "message": "Container exited with status 0",
        }

        app = App()

        @app.route(["/api/v1"], RouteTypes.HTTP, methods=["POST"])
        async def api_v1(request: Request):
            event_str = json.dumps(mesos_state_finished_event_data)
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
            self.assertEqual(
                events[0].dict(skip_defaults=True), asgard_event_expected_data
            )

    async def test_creates_client_session_with_timeout_options(self):
        with mock.patch.object(
            mesos_consumer_module, "ClientSession"
        ) as client_session_mock:
            client_session_mock.return_value = CoroutineMock(
                post=CoroutineMock()
            )
            consumer = MesosEventConsumer(
                HTTPConnection(urls=settings.MESOS_MASTER_URLS)
            )
            await consumer.connect()
            client_session_mock.assert_called_with(timeout=timeout_config)

    async def test_use_next_mesos_urls_if_needed(self):
        with aioresponses() as rsps:
            rsps.post(
                f"{settings.MESOS_MASTER_URLS[0]}/api/v1",
                exception=ClientTimeout("timeout"),
            )
            rsps.post(
                f"{settings.MESOS_MASTER_URLS[1]}/api/v1",
                exception=ClientTimeout("timeout"),
            )
            consumer = MesosEventConsumer(
                HTTPConnection(urls=settings.MESOS_MASTER_URLS)
            )
            await consumer.connect()
        self.assertTrue(True)
