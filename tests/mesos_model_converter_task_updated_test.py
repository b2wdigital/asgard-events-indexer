import json

from asynctest.mock import ANY

from indexer.mesos.models.converters.taskupdated import (
    MesosTaskUpdatedEventConverter,
)
from indexer.mesos.models.taskupdated import MesosTaskUpdatedEvent
from indexer.models.event import BackendInfoTypes, EventSourceSpec, TaskStatus
from tests.base import FIXTURE_DIR, BaseTestCase

mesos_state_finished_event_data = {
    "state": "TASK_FINISHED",
    "status": {
        "agent_id": {"value": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"},
        "message": "Container exited with status 0",
        "source": "SOURCE_EXECUTOR",
        "state": "TASK_FINISHED",
        "task_id": {"value": "ct:1578593280013:0:asgard-heimdall:"},
        "timestamp": 1_578_685_955,
    },
}


mesos_state_failed_event_data = {
    "state": "TASK_FAILED",
    "status": {
        "agent_id": {"value": "4783cf15-4fb1-4c75-90fe-44eeec5258a7-S28"},
        "message": "Failed to launch container: Failed to run 'docker -H unix:///var/run/docker.sock pull debian7': exited with status 1; stderr='Error response from daemon: pull access denied for debian7, repository does not exist or may require 'docker login'\n'",
        "reason": "REASON_CONTAINER_LAUNCH_FAILED",
        "source": "SOURCE_EXECUTOR",
        "state": "TASK_FAILED",
        "task_id": {
            "value": "asgard_sleep.4bae57ad-33b7-11ea-a2e5-02429217540f"
        },
        "timestamp": 1_578_667_533,
    },
}


class MesosTaskUpdatedConverterTest(BaseTestCase):
    async def test_convert_to_asgard_model_state_finished(self):
        """
        Cria um Event a partir de um evento do mesos com state TASK_FINISHED
        """
        asgard_event_expected_data = {
            "id": ANY,
            "date": "2020-01-10T19:52:35+00:00",
            "appname": "heimdall",
            "namespace": "asgard",
            "backend_info": {"name": BackendInfoTypes.CHRONOS},
            "task": {"id": "heimdall"},
            "agent": {"id": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"},
            "status": "TASK_FINISHED",
            "source": EventSourceSpec.SOURCE_EXECUTOR,
            "message": "Container exited with status 0",
        }

        mesos_event = MesosTaskUpdatedEvent(**mesos_state_finished_event_data)
        asgard_event = MesosTaskUpdatedEventConverter.to_asgard_model(
            mesos_event
        )
        self.assertEqual(
            asgard_event.dict(skip_defaults=True), asgard_event_expected_data
        )

    async def test_convert_to_asgard_model_state_failed(self):
        """
        Cria um Event a partir de um evento do mesos com state TASK_FAILED
        """
        asgard_event_expected_data = {
            "id": ANY,
            "date": "2020-01-10T14:45:33+00:00",
            "appname": "sleep",
            "namespace": "asgard",
            "backend_info": {"name": BackendInfoTypes.MARATHON},
            "task": {"id": "sleep.4bae57ad-33b7-11ea-a2e5-02429217540f"},
            "agent": {"id": "4783cf15-4fb1-4c75-90fe-44eeec5258a7-S28"},
            "status": "TASK_FAILED",
            "source": EventSourceSpec.SOURCE_EXECUTOR,
            "error": {
                "message": "Failed to launch container: Failed to run 'docker -H unix:///var/run/docker.sock pull debian7': exited with status 1; stderr='Error response from daemon: pull access denied for debian7, repository does not exist or may require 'docker login'\n'",
                "reason": "REASON_CONTAINER_LAUNCH_FAILED",
            },
        }

        mesos_event = MesosTaskUpdatedEvent(**mesos_state_failed_event_data)
        asgard_event = MesosTaskUpdatedEventConverter.to_asgard_model(
            mesos_event
        )
        self.assertEqual(
            asgard_event.dict(skip_defaults=True), asgard_event_expected_data
        )

    async def test_convert_to_asgard_model_state_running(self):
        """
        Vamos fazer a conversão normalmentem, mas vamos pegar alguns dados do campo
        `data`. Lá temos dados como nome do container, portas alocadas a essa intância, etc.
        """

        asgard_event_expected_data = {
            "agent": {"id": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"},
            "appname": "heimdall",
            "backend_info": {"name": "Mesos/Chronos"},
            "date": "2020-01-10T14:46:08+00:00",
            "id": ANY,
            "namespace": "asgard",
            "source": EventSourceSpec.SOURCE_EXECUTOR,
            "status": TaskStatus.TASK_RUNNING,
            "task": {"id": "heimdall"},
            "container_info": {
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
            },
        }

        event_state_running_dict = json.loads(
            open(FIXTURE_DIR + "/mesos_state_running_event_data.json").read()
        )
        mesos_task_updated_event = MesosTaskUpdatedEvent(
            **event_state_running_dict["task_updated"]
        )
        asgard_event = MesosTaskUpdatedEventConverter.to_asgard_model(
            mesos_task_updated_event
        )
        self.assertEqual(
            asgard_event.dict(skip_defaults=True), asgard_event_expected_data
        )

    async def test_task_updated_state_running_without_swappiness(self):

        event_state_running_dict = json.loads(
            open(
                FIXTURE_DIR
                + "/mesos_state_running_event_data_without_swappiness.json"
            ).read()
        )
        mesos_task_updated_event = MesosTaskUpdatedEvent(
            **event_state_running_dict["task_updated"]
        )
        asgard_event = MesosTaskUpdatedEventConverter.to_asgard_model(
            mesos_task_updated_event
        )
        self.assertIsNone(
            asgard_event.container_info.resources.memory_swappiness
        )
