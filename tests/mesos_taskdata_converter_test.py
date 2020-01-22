import json

from indexer.mesos.models.converters.spec import MesosTaskDataSpecConverter
from indexer.mesos.models.event import MesosEvent
from tests.base import BaseTestCase, FIXTURE_DIR


class MesosTaskDataConverterTest(BaseTestCase):
    async def setUp(self):

        mesos_event_state_running_dict = json.loads(
            open(FIXTURE_DIR + "/mesos_state_running_event_data.json").read()
        )
        mesos_event = MesosEvent(**mesos_event_state_running_dict)
        self.mesos_task_data_spec = mesos_event.task_details()

    async def test_convert_to_asgard_container_info_spec(self):
        asgard_container_info_expected_data = {
            "pid": 32306,
            "running": True,
            "exit_code": 0,
            "error": "",
            "hostname": "general-3",
            "image": "debian:stretch-slim",
            "name": "mesos-3b4d62e0-e955-4483-b1de-d48f127491ce",
            "resources": {
                "cpu_quota": 10000,
                "cpu_shares": 102,
                "memory_swap": -1,
                "memory_swappiness": 25,
            },
            "labels": [
                {
                    "name": "MESOS_TASK_ID",
                    "value": "asgard_sleep.0be2b6b0-37a5-11ea-a2e5-02429217540f",
                },
                {"name": "hollowman.appname", "value": "/asgard/sleep"},
            ],
            "volumes": [
                {
                    "container_path": "/tmp/bla",
                    "host_path": "/tmp/bla",
                    "mode": "rw",
                },
                {
                    "container_path": "/mnt/mesos/sandbox",
                    "host_path": "/tmp/mesos/slaves/4783cf15-4fb1-4c75-90fe-44eeec5258a7-S28/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0000/executors/asgard_sleep.0be2b6b0-37a5-11ea-a2e5-02429217540f/runs/3b4d62e0-e955-4483-b1de-d48f127491ce",
                    "mode": "",
                },
            ],
        }

        asgard_container_info_spec = MesosTaskDataSpecConverter.to_asgard_model(
            self.mesos_task_data_spec
        )

        self.assertEqual(
            asgard_container_info_spec.dict(skip_defaults=True),
            asgard_container_info_expected_data,
        )
