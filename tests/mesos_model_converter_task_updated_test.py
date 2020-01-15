from asynctest import skip
from asynctest.mock import ANY
from freezegun import freeze_time

from indexer.mesos.models.converters.taskupdated import (
    MesosTaskUpdatedEventConverter,
)
from indexer.mesos.models.taskupdated import MesosTaskUpdatedEvent
from indexer.models.event import BackendInfoTypes, EventSourceSpec
from tests.base import BaseTestCase

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

    @skip("")
    async def test_convert_to_asgard_model_state_running(self):
        """
        Vamos fazer a conversão normalmentem, mas vamos pegar alguns dados do campo
        `data`. Lá temos dados como nome do container, portas alocadas a essa intância, etc.
        Dados que vamos pegar do campo `data`:
         - State/Running
         - State/Pid
         - Name (que é ocontainer name. Remover a `/` do início)
         - HostConfig/
            - CpuShares
            - CpuQuota
            - MemorySwap
            - MemorySwappiness
         - Config/
            - Labels
            - Hostname
            - Env (vale a pena? Corremos o risco de indexar informacões sensíveis)
            - Image
            - Volumes (Pegar um exemplo)
        """
        self.fail()
