import os
from asyncio import TimeoutError
from typing import List

from aiohttp import ClientError
from asynctest import mock
from asynctest.mock import CoroutineMock

from indexer import consumer as consumer_module
from indexer import writter as writter_module
from indexer.conf import Settings
from indexer.connection import HTTPConnection
from indexer.consumer import Consumer
from indexer.models.event import Event
from indexer.writter import OutputWritter, ElasticSearchOutputWritter
from tests.base import BaseTestCase, LOGGER_MOCK


class MyConsumer(Consumer):
    def __init__(self, conn: HTTPConnection):
        Consumer.__init__(self, conn)
        self.all_events = []

    async def connect(self):
        pass

    async def events(self):
        yield 10
        yield 20

    async def write_output(self, events: List[Event]) -> None:
        self.all_events.extend(events)


class StdOutConsumer(Consumer):
    async def connect(self):
        pass

    async def events(self):
        pass


class EventConsumerTest(BaseTestCase):
    async def test_reconnect_if_exception_on_current_connection(self):

        consumer = MyConsumer(HTTPConnection(urls=["http://127.0.0.1:5050"]))
        with mock.patch.object(
            consumer, "events", side_effect=ClientError()
        ), mock.patch.object(
            consumer, "should_run", side_effect=[True, True, False]
        ), mock.patch.object(
            consumer, "connect", CoroutineMock()
        ) as connect_mock:
            await consumer.start()
            self.assertEqual(2, connect_mock.await_count)

    async def test_reconnect_if_timeout_exception_on_consume(self):

        consumer = MyConsumer(HTTPConnection(urls=["http://127.0.0.1:5050"]))
        with mock.patch.object(
            consumer, "events", side_effect=TimeoutError()
        ), mock.patch.object(
            consumer, "should_run", side_effect=[True, True, False]
        ), mock.patch.object(
            consumer, "connect", CoroutineMock()
        ) as connect_mock:
            await consumer.start()
            self.assertEqual(2, connect_mock.await_count)

    async def test_logs_exception_in_event_processing(self):
        consumer = MyConsumer(HTTPConnection(urls=["http://127.0.0.1:5050"]))
        with mock.patch(
            "indexer.consumer.logger", LOGGER_MOCK
        ) as logger_mock, mock.patch.object(
            consumer, "connect", side_effect=ClientError("Error Connecting")
        ), mock.patch.object(
            consumer, "should_run", side_effect=[True, False]
        ):
            await consumer.start()
            logger_mock.exception.assert_awaited_with(
                {
                    "event": "exception-consuming-events",
                    "exc": "Error Connecting",
                }
            )

    async def test_calls_on_record_method(self):
        """
        Confere que quando recebemos um novo chunk (self.on_chunk())
        fazemos o parsing e chamamos o método self.on_record(...) passando
        o Record já parseado.
        """

        consumer = MyConsumer(HTTPConnection(urls=["http://127.0.0.1:5050"]))

        with mock.patch(
            "indexer.consumer.logger", LOGGER_MOCK
        ), mock.patch.object(consumer, "should_run", side_effect=[True, False]):
            await consumer.start()
            self.assertEqual([10, 20], consumer.all_events)

    async def test_should_run(self):
        consumer = MyConsumer(HTTPConnection(urls=["http://127.0.0.1:5050"]))
        self.assertTrue(consumer.should_run())

        consumer._run = False
        self.assertFalse(consumer.should_run())

    async def test_write_output_calls_writter_instance(self):

        with mock.patch.dict(os.environ, TEST_OUTPUT_TO_STDOUT="1"):
            settings_stub = Settings()

            with mock.patch.object(
                consumer_module, "settings", settings_stub
            ), mock.patch.object(
                writter_module, "logger", LOGGER_MOCK
            ) as logger_mock:
                event_mock = mock.MagicMock()
                event_mock.dict.return_value = {"status": "TASK_RUNNING"}

                consumer = StdOutConsumer(
                    HTTPConnection(urls=["http://127.0.0.1:5050"])
                )
                await consumer.write_output([event_mock])
                logger_mock.info.assert_awaited_with({"status": "TASK_RUNNING"})

    async def test_consumer_instantiate_es_writter_if_env_url_set(self):

        import json

        es_urls = json.dumps(["http://127.0.0.1:5050", "http://10.0.0.1:5050"])
        with mock.patch.dict(os.environ, TEST_ES_OUTPUT_URLS=es_urls):
            settings_stub = Settings()
            with mock.patch.object(consumer_module, "settings", settings_stub):
                consumer = StdOutConsumer(
                    HTTPConnection(urls=["http://127.0.0.1:5050"])
                )
                self.assertTrue(
                    isinstance(consumer.output[0], ElasticSearchOutputWritter)
                )

    async def test_consumer_instantiate_stdout_writter_if_env_url_set(self):

        with mock.patch.dict(os.environ, TEST_OUTPUT_TO_STDOUT="1"):
            settings_stub = Settings()
            with mock.patch.object(consumer_module, "settings", settings_stub):
                consumer = StdOutConsumer(
                    HTTPConnection(urls=["http://127.0.0.1:5050"])
                )
                self.assertTrue(isinstance(consumer.output[0], OutputWritter))
