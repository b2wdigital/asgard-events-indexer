from indexer.models.event import BackendInfoTypes
from indexer.models.util import get_backend_info
from tests.base import BaseTestCase


class ModelsUtilTest(BaseTestCase):
    async def test_get_backend_info_types(self):
        self.assertEqual(
            BackendInfoTypes.CHRONOS,
            get_backend_info("ct:1578492720011:0:asgard-my-other-app-name:"),
        )
        self.assertEqual(
            BackendInfoTypes.MARATHON,
            get_backend_info(
                "sieve_sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
