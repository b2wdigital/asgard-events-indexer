from asynctest import mock

from indexer import writter as writter_module
from indexer.writter import OutputWritter
from tests.base import BaseTestCase, LOGGER_MOCK


class BaseOutputWritterTest(BaseTestCase):
    async def test_logs_all_events(self):
        with mock.patch.object(
            writter_module, "logger", LOGGER_MOCK
        ) as logger_mock:
            writter = OutputWritter()
            event_mock = mock.MagicMock()
            event_mock.dict.return_value = {"status": "TASK_RUNNING"}
            await writter.write([event_mock])

            logger_mock.info.assert_awaited_with({"status": "TASK_RUNNING"})
