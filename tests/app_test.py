from asynctest import mock
from asynctest.mock import CoroutineMock, MagicMock

from indexer import app
from indexer.conf import settings
from indexer.connection import HTTPConnection
from tests.base import BaseTestCase


class AppTest(BaseTestCase):
    async def test_creates_a_consumer_with_correct_urls(self):
        consumer_instance_mock = CoroutineMock(start=CoroutineMock())
        with mock.patch.object(
            app,
            "MesosEventConsumer",
            MagicMock(return_value=consumer_instance_mock),
        ) as consumer_mock:
            await app.main()
            consumer_instance_mock.start.assert_awaited()
            consumer_mock.assert_called_with(
                HTTPConnection(urls=settings.MESOS_MASTER_URLS)
            )
