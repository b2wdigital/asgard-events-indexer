from unittest.mock import ANY

from freezegun import freeze_time

from indexer.mesos.models.converters.taskadded import (
    MesosTaskAddedEventConverter,
    get_task_namespace,
    remove_task_namespace,
    get_appname,
)
from indexer.mesos.models.taskadded import MesosTaskAddedEvent
from tests.base import BaseTestCase


class MesosEventModelConverterUtilTest(BaseTestCase):
    async def test_remove_namespace_from_task_id(self):
        """
        Dado um nome de task do mesos (criado pelo Marathon), retornamos
        o nome da task sem o namespace.
        """
        self.assertEqual(
            "sleep.c73b9af1-1abb-11ea-a2e5-02429217540f",
            remove_task_namespace(
                "sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f",
            remove_task_namespace(
                "other_sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f",
            remove_task_namespace(
                "sieve_sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "heimdall-other-app",
            remove_task_namespace(
                "ct:1578492720011:0:asgard-heimdall-other-app:"
            ),
        )
        self.assertEqual(
            "heimdall",
            remove_task_namespace("ct:1578492720011:0:asgard-heimdall:"),
        )

    async def test_get_namespace_from_task_id(self):
        """
        Dado o nome de uma task, retornamos o namespace que ela pertence
        """
        self.assertEqual(
            "sieve",
            get_task_namespace(
                "sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "other",
            get_task_namespace(
                "other_sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "sieve",
            get_task_namespace(
                "sieve_sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "asgard", get_task_namespace("ct:1578492720011:0:asgard-heimdall:")
        )

    async def test_get_appname(self):
        self.assertEqual(
            "sleep",
            get_appname("sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"),
        )
        self.assertEqual(
            "sieve/sleep",
            get_appname(
                "other_sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "sieve/sleep",
            get_appname(
                "sieve_sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
            ),
        )
        self.assertEqual(
            "heimdall", get_appname("ct:1578492720011:0:asgard-heimdall:")
        )
        self.assertEqual(
            "my-other-app-name",
            get_appname("ct:1578492720011:0:asgard-my-other-app-name:"),
        )
