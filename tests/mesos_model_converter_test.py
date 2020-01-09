from unittest.mock import ANY

from freezegun import freeze_time

from indexer.mesos.models import MesosTaskAddedEvent
from indexer.mesos.models.converters.taskadded import (
    MesosTaskAddedEventConverter,
    get_task_namespace,
    remove_task_namespace,
    get_appname,
)
from tests.base import BaseTestCase

mesos_event_data = {
    "task": {
        "agent_id": {"value": "79ad3a13-b567-4273-ac8c-30378d35a439-S14522"},
        "container": {
            "docker": {
                "force_pull_image": True,
                "image": "alpine",
                "network": "BRIDGE",
                "parameters": [
                    {"key": "label", "value": "hollowman.appname=/sieve/sleep"},
                    {
                        "key": "label",
                        "value": "MESOS_TASK_ID=sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f",
                    },
                ],
                "privileged": False,
            },
            "type": "DOCKER",
        },
        "discovery": {
            "name": "sleep.sieve",
            "ports": {},
            "visibility": "FRAMEWORK",
        },
        "framework_id": {"value": "4783cf15-4fb1-4c75-90fe-44eeec5258a7-0000"},
        "labels": {
            "labels": [{"key": "hollowman.default_scale", "value": "2"}]
        },
        "name": "sleep.sieve",
        "resources": [
            {
                "allocation_info": {"role": "*"},
                "name": "cpus",
                "scalar": {"value": 0.1},
                "type": "SCALAR",
            },
            {
                "allocation_info": {"role": "*"},
                "name": "mem",
                "scalar": {"value": 32},
                "type": "SCALAR",
            },
        ],
        "state": "TASK_STAGING",
        "task_id": {
            "value": "sieve_sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"
        },
    }
}


class MesosEventModelConverter(BaseTestCase):
    async def test_convert_to_asgard_model_task_added(self):

        asgard_event_data_expected = {
            "id": ANY,
            "date": ANY,
            "appname": "sleep",
            "namespace": "sieve",
            "backend_info": {"name": "Mesos"},
            "task": {"id": "sleep.c73b9af1-1abb-11ea-a2e5-02429217540f"},
            "agent": {"id": "79ad3a13-b567-4273-ac8c-30378d35a439-S14522"},
            "status": "TASK_STAGING",
            "error": None,
            "source": "MASTER",
        }
        mesos_event = MesosTaskAddedEvent(**mesos_event_data)
        asgard_event = MesosTaskAddedEventConverter.to_asgard_model(mesos_event)
        self.assertEqual(asgard_event.dict(), asgard_event_data_expected)

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

    @freeze_time("2018-06-16T10:16:00-03:00")
    async def test_check_date_has_tzinfo(self):
        """
        Checa que o campo `date` é uma data UTC serializada com informações
        sobre a timezone: "...+00:00"
        """
        mesos_event = MesosTaskAddedEvent(**mesos_event_data)
        asgard_event = MesosTaskAddedEventConverter.to_asgard_model(mesos_event)

        self.assertEqual(asgard_event.date, "2018-06-16T13:16:00+00:00")
