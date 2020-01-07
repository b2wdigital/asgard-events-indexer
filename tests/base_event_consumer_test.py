from asyncio import TimeoutError
from typing import List

from aiohttp import ClientError
from asynctest import mock
from asynctest.mock import CoroutineMock
from tests.base import BaseTestCase, LOGGER_MOCK

from indexer.conf import logger
from indexer.connection import HTTPConnection
from indexer.consumer import Consumer
from indexer.models.event import Event


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
        ) as connect_mock, mock.patch.object(
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
        ) as connect_mock, mock.patch.object(
            consumer, "should_run", side_effect=[True, False]
        ):
            await consumer.start()
            self.assertEqual([10, 20], consumer.all_events)

    async def test_should_run(self):
        consumer = MyConsumer(HTTPConnection(urls=["http://127.0.0.1:5050"]))
        self.assertTrue(consumer.should_run())

        consumer._run = False
        self.assertFalse(consumer.should_run())
