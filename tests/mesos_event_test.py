import json

import pytest
from asynctest import skip
from tests.base import BaseTestCase, FIXTURE_DIR

from indexer.mesos.models.event import MesosEvent

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


class MesosEventModelTest(BaseTestCase):
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

    async def test_can_parse_task_updated_state_running(self):
        mesos_state_running_event_data = open(
            f"{FIXTURE_DIR}/mesos_state_running_event_data.json"
        ).read()
        mesos_state_running_event_dict = json.loads(
            mesos_state_running_event_data
        )
        mesos_event = MesosEvent(**mesos_state_running_event_dict)
        self.assertEqual(
            mesos_event.task_updated.dict(skip_defaults=True),
            mesos_state_running_event_dict["task_updated"],
        )
