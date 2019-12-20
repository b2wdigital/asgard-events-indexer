from indexer.conf import settings
from tests.base import BaseTestCase


class ConfTest(BaseTestCase):
    async def test_load_mesos_urls(self):
        self.assertEqual(settings.MESOS_MASTER_URLS, ["http://127.0.0.1:5050"])
