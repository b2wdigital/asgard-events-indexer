import json

from indexer.mesos.models.event import MesosEvent
from tests.base import BaseTestCase, FIXTURE_DIR

mesos_state_finished_event_data = {
    "task_updated": {
        "state": "TASK_FINISHED",
        "status": {
            "agent_id": {"value": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"},
            "message": "Container exited with status 0",
            "source": "SOURCE_EXECUTOR",
            "state": "TASK_FINISHED",
            "task_id": {"value": "ct:1578593280013:0:asgard-heimdall:"},
            "timestamp": 1_578_593_296,
        },
    },
    "type": "TASK_UPDATED",
}


mesos_state_failed_event_data = {
    "task_updated": {
        "state": "TASK_FAILED",
        "status": {
            "agent_id": {"value": "4783cf15-4fb1-4c75-90fe-44eeec5258a7-S28"},
            "message": "Container exited with status 137",
            "source": "SOURCE_EXECUTOR",
            "state": "TASK_FAILED",
            "task_id": {
                "value": "asgard_sleep.4bae57ad-33b7-11ea-a2e5-02429217540f"
            },
            "timestamp": 1_578_667_533,
        },
    },
    "type": "TASK_UPDATED",
}


mesos_task_added_event_data = {
    "task_added": {
        "task": {
            "agent_id": {"value": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"},
            "container": {"type": "DOCKER"},
            "name": "ChronosTask:asgard-heimdall",
            "state": "TASK_STAGING",
            "task_id": {"value": "ct:1579100880008:0:asgard-heimdall:"},
        }
    },
    "type": "TASK_ADDED",
}


class MesosEventModelTest(BaseTestCase):
    async def setUp(self):

        mesos_state_running_event_data = open(
            f"{FIXTURE_DIR}/mesos_state_running_event_data.json"
        ).read()
        self.mesos_state_running_event_dict = json.loads(
            mesos_state_running_event_data
        )

    async def test_can_parse_task_updated_state_finished(self):
        mesos_event = MesosEvent(**mesos_state_finished_event_data)
        self.assertEqual(
            mesos_event.task_updated.dict(skip_defaults=True),
            mesos_state_finished_event_data["task_updated"],
        )

    async def test_can_parse_task_updated_state_failed(self):
        mesos_event = MesosEvent(**mesos_state_failed_event_data)
        self.assertEqual(
            mesos_event.task_updated.dict(skip_defaults=True),
            mesos_state_failed_event_data["task_updated"],
        )

    async def test_can_parse_task_updated_state_failed_no_task_data(self):
        mesos_event = MesosEvent(**mesos_state_failed_event_data)
        self.assertIsNone(mesos_event.task_details())

    async def test_can_parse_task_added_no_task_data(self):
        mesos_event = MesosEvent(**mesos_task_added_event_data)
        self.assertIsNone(mesos_event.task_details())

    async def test_can_parse_task_updated_state_running(self):

        mesos_event = MesosEvent(**self.mesos_state_running_event_dict)
        self.assertEqual(
            mesos_event.task_updated.dict(skip_defaults=True),
            self.mesos_state_running_event_dict["task_updated"],
        )

    async def test_parse_optional_data_field_state_running(self):
        """
        Dados que vamos pegar do campo `data`:
         - State/
            - Running
            - Pid
            - ExitCode
            - Error
         - Name (que é ocontainer name. Remover a `/` do início)
         - Mounts
         - HostConfig/
            - CpuShares
            - CpuQuota
            - MemorySwap
            - MemorySwappiness
         - Config/
            - Hostname
            - Image
            - Labels
        """
        mesos_event = MesosEvent(**self.mesos_state_running_event_dict)
        task_details = mesos_event.task_details()

        # State
        self.assertEqual(True, task_details.State.Running)
        self.assertEqual(32306, task_details.State.Pid)
        self.assertEqual(0, task_details.State.ExitCode)
        self.assertEqual("", task_details.State.Error)

        # Mounts
        self.assertEqual(2, len(task_details.Mounts))
        self.assertEqual("bind", task_details.Mounts[0].Type)
        self.assertEqual("/tmp/bla", task_details.Mounts[0].Source)
        self.assertEqual("/tmp/bla", task_details.Mounts[0].Destination)
        self.assertEqual("rw", task_details.Mounts[0].Mode)
        self.assertEqual(True, task_details.Mounts[0].RW)

        # HostConfig
        self.assertEqual(102, task_details.HostConfig.CpuShares)
        self.assertEqual(10000, task_details.HostConfig.CpuQuota)
        self.assertEqual(-1, task_details.HostConfig.MemorySwap)
        self.assertEqual(25, task_details.HostConfig.MemorySwappiness)

        self.assertEqual(
            "mesos-3b4d62e0-e955-4483-b1de-d48f127491ce", task_details.Name
        )

        # config
        self.assertEqual("general-3", task_details.config.Hostname)
        self.assertEqual("debian:stretch-slim", task_details.config.Image)
        self.assertEqual(
            {
                "MESOS_TASK_ID": "asgard_sleep.0be2b6b0-37a5-11ea-a2e5-02429217540f",
                "hollowman.appname": "/asgard/sleep",
            },
            task_details.config.Labels,
        )
