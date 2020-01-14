from unittest.mock import ANY

from freezegun import freeze_time

from indexer.mesos.models.converters.taskadded import (
    MesosTaskAddedEventConverter,
)
from indexer.mesos.models.taskadded import MesosTaskAddedEvent
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


class MesosModelConverterTaskAddedEventTest(BaseTestCase):
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
            "source": "MASTER",
        }
        mesos_event = MesosTaskAddedEvent(**mesos_event_data)
        asgard_event = MesosTaskAddedEventConverter.to_asgard_model(mesos_event)
        self.assertEqual(
            asgard_event.dict(skip_defaults=True), asgard_event_data_expected
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
