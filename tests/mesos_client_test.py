import json

from aiohttp.client import ClientError, ClientSession
from aioresponses import aioresponses
from asynctest import TestCase

from indexer.conf import settings
from indexer.connection import HTTPConnection
from indexer.mesos.client import (
    CompletedTaskInfo,
    MesosClient,
    AgentNotFoundException,
    NoMoreMesosServersException,
)
from indexer.mesos.models.spec import TaskIdSpec, AgentIdSpec
from tests.base import FIXTURE_DIR

mesos_api_response_data = {
    "recovered_slaves": [],
    "slaves": [
        {
            "hostname": "10.234.172.50",
            "id": "79ad3a13-b567-4273-ac8c-30378d35a439-S6563",
            "port": 5051,
        }
    ],
}

mesos_api_response_empty_data = {"recovered_slaves": [], "slaves": []}


task_file_list_less_than_4k_size = [
    {
        "path": "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0000/executors/asgard_monitoring_prometheus.32f98443-061e-11ea-bbc8-0242e96b0c6b/runs/d341507d-1ca9-4133-9d78-fe05c7524333/stderr",
        "size": 520,
    },
    {
        "path": "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0000/executors/asgard_monitoring_prometheus.32f98443-061e-11ea-bbc8-0242e96b0c6b/runs/d341507d-1ca9-4133-9d78-fe05c7524333/stdout",
        "size": 3287,
    },
]


task_file_list_more_than_4k_size = [
    {
        "path": "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0000/executors/asgard_monitoring_prometheus.32f98443-061e-11ea-bbc8-0242e96b0c6b/runs/d341507d-1ca9-4133-9d78-fe05c7524333/stderr",
        "size": 8192,
    },
    {
        "path": "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0000/executors/asgard_monitoring_prometheus.32f98443-061e-11ea-bbc8-0242e96b0c6b/runs/d341507d-1ca9-4133-9d78-fe05c7524333/stdout",
        "size": 12798,
    },
]


class MesosClientTest(TestCase):
    use_default_loop = True

    async def setUp(self):
        self.agent_id = "79ad3a13-b567-4273-ac8c-30378d35a439-S6563"
        conn = HTTPConnection(urls=settings.MESOS_MASTER_URLS)
        self.mesos_client = MesosClient(ClientSession(), conn)

        self.agent_state_dict = json.loads(
            open(f"{FIXTURE_DIR}/mesos_slave_state.json").read()
        )
        self.agent_addr = "http://10.0.0.1:5051"

    async def test_get_slave_address_slave_exist(self):
        with aioresponses() as rsps:
            rsps.get(
                f"{settings.MESOS_MASTER_URLS[0]}/slaves?slave_id={self.agent_id}",
                status=200,
                payload=mesos_api_response_data,
            )
            agent_addr = await self.mesos_client.get_agent_address(
                AgentIdSpec(value=self.agent_id)
            )
            self.assertEqual("http://10.234.172.50:5051", agent_addr)

    async def test_get_slave_address_slave_not_found(self):
        """
        Lança exception caso um slave não seja encontrado através do seu ID
        """
        with aioresponses() as rsps:
            rsps.get(
                f"{settings.MESOS_MASTER_URLS[0]}/slaves?slave_id={self.agent_id}",
                status=200,
                payload=mesos_api_response_empty_data,
            )
            with self.assertRaises(AgentNotFoundException):
                await self.mesos_client.get_agent_address(
                    AgentIdSpec(value=self.agent_id)
                )

    async def test_client_error_should_try_other_urls(self):
        with aioresponses() as rsps:
            rsps.get(
                f"{settings.MESOS_MASTER_URLS[0]}/slaves?slave_id={self.agent_id}",
                exception=ClientError(),
            )
            rsps.get(
                f"{settings.MESOS_MASTER_URLS[1]}/slaves?slave_id={self.agent_id}",
                status=200,
                payload=mesos_api_response_data,
            )
            agent_addr = await self.mesos_client.get_agent_address(
                AgentIdSpec(value=self.agent_id)
            )
            self.assertEqual("http://10.234.172.50:5051", agent_addr)

    async def test_return_empty_dict_if_no_mesos_is_reachable(self):
        with aioresponses() as rsps:
            rsps.get(
                f"{settings.MESOS_MASTER_URLS[0]}/slaves?slave_id={self.agent_id}",
                exception=ClientError(),
            )
            rsps.get(
                f"{settings.MESOS_MASTER_URLS[1]}/slaves?slave_id={self.agent_id}",
                exception=ClientError(),
            )
            with self.assertRaises(NoMoreMesosServersException):
                await self.mesos_client.get_agent_address(
                    AgentIdSpec(value=self.agent_id)
                )

    async def test_task_info_active_framework_completed_executors(self):
        task_id = TaskIdSpec(value="ct:1581360840007:0:asgard-my-app:")
        expected_directory_value = "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0001/executors/ct:1581360840007:0:asgard-heimdall:/runs/2bca2a9b-2eea-48a9-9b18-b69b1c5118f7"
        with aioresponses() as rsps:
            rsps.get(
                f"{self.agent_addr}/state",
                status=200,
                payload=self.agent_state_dict,
            )
            task_info = await self.mesos_client.get_task_info(
                self.agent_addr, task_id
            )
            self.assertIsNotNone(task_info)
            self.assertEqual(expected_directory_value, task_info.directory)
            self.assertEqual(task_id.value, task_info.id)

    async def test_task_info_completed_framework_completed_executors(self):
        task_id = TaskIdSpec(value="asgard_webapp_apis_billing.2bca2a9b")
        expected_directory_value = "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0001/executors/ct:1581360900080:0:asgard-heimdall:/runs/238657cd-0f2b-47a7-a48d-ffcf453be31d"
        with aioresponses() as rsps:
            rsps.get(
                f"{self.agent_addr}/state",
                status=200,
                payload=self.agent_state_dict,
            )
            task_info = await self.mesos_client.get_task_info(
                self.agent_addr, task_id
            )
            self.assertIsNotNone(task_info)
            self.assertEqual(expected_directory_value, task_info.directory)
            self.assertEqual(task_id.value, task_info.id)

    async def test_task_info_task_not_found(self):
        task_id = TaskIdSpec(value="not-found-task-id")
        with aioresponses() as rsps:
            rsps.get(
                f"{self.agent_addr}/state",
                status=200,
                payload=self.agent_state_dict,
            )
            task_info = await self.mesos_client.get_task_info(
                self.agent_addr, task_id
            )
            self.assertIsNone(task_info)

    async def test_get_task_file_size_less_than_4k(self):
        task_id = "task-id"
        directory = "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0001/executors/ct:1581360780082:0:asgard-heimdall:/runs/4d70dbf3-8131-402b-a026-a2d8e7f7ae7e"
        task_info = CompletedTaskInfo(id=task_id, directory=directory)
        with aioresponses() as rsps:
            rsps.get(
                f"{self.agent_addr}/files/browse?path={directory}",
                status=200,
                payload=task_file_list_less_than_4k_size,
            )
            rsps.get(
                f"{self.agent_addr}/files/browse?path={directory}",
                status=200,
                payload=task_file_list_less_than_4k_size,
            )
            rsps.get(
                f"{self.agent_addr}/files/read?path={directory}/stdout&length=4096&offset=0",
                status=200,
                payload={"data": "stdout-output from task", "offset": 0},
            )
            rsps.get(
                f"{self.agent_addr}/files/read?path={directory}/stderr&length=4096&offset=0",
                status=200,
                payload={"data": "stderr-output from task", "offset": 0},
            )

            task_output_data = await self.mesos_client.get_task_output_data(
                self.agent_addr, task_info
            )

            self.assertEqual("stdout-output from task", task_output_data.stdout)
            self.assertEqual("stderr-output from task", task_output_data.stderr)

    async def test_get_task_stdout_file_size_more_than_4k(self):
        task_id = "task-id"
        directory = "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0001/executors/ct:1581360780082:0:asgard-heimdall:/runs/4d70dbf3-8131-402b-a026-a2d8e7f7ae7e"
        task_info = CompletedTaskInfo(id=task_id, directory=directory)
        with aioresponses() as rsps:
            rsps.get(
                f"{self.agent_addr}/files/browse?path={directory}",
                status=200,
                payload=task_file_list_more_than_4k_size,
            )
            rsps.get(
                f"{self.agent_addr}/files/browse?path={directory}",
                status=200,
                payload=task_file_list_more_than_4k_size,
            )

            stdout_offset = 12798 - settings.TASK_FILE_CONTENT_LENGTH
            rsps.get(
                f"{self.agent_addr}/files/read?path={directory}/stdout&length=4096&offset={stdout_offset}",
                status=200,
                payload={
                    "data": "stderr-output from task more than 4k file",
                    "offset": 0,
                },
            )

            stderr_offset = 8192 - settings.TASK_FILE_CONTENT_LENGTH
            rsps.get(
                f"{self.agent_addr}/files/read?path={directory}/stderr&length=4096&offset={stderr_offset}",
                status=200,
                payload={
                    "data": "stderr-output from task more than 4k file",
                    "offset": 0,
                },
            )
            task_output_data = await self.mesos_client.get_task_output_data(
                self.agent_addr, task_info
            )

            self.assertEqual(
                "stderr-output from task more than 4k file",
                task_output_data.stdout,
            )

            self.assertEqual(
                "stderr-output from task more than 4k file",
                task_output_data.stderr,
            )

    async def test_get_task_file_not_found(self):
        """
        O segundo request é parte do assert. A conferência é o valor do offset.
        """
        task_id = "task-id"
        directory = "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0001/executors/ct:1581360780082:0:asgard-heimdall:/runs/4d70dbf3-8131-402b-a026-a2d8e7f7ae7e"
        task_info = CompletedTaskInfo(id=task_id, directory=directory)
        with aioresponses() as rsps:
            rsps.get(
                f"{self.agent_addr}/files/browse?path={directory}",
                status=200,
                payload=[{"path": f"{directory}/other-file", "size": 42}],
            )
            rsps.get(
                f"{self.agent_addr}/files/browse?path={directory}",
                status=200,
                payload=[{"path": f"{directory}/other-file", "size": 42}],
            )
            rsps.get(
                f"{self.agent_addr}/files/read?path={directory}/stdout&length=4096&offset=0",
                status=200,
                payload={"data": "stdout-output from task", "offset": 0},
            )
            rsps.get(
                f"{self.agent_addr}/files/read?path={directory}/stderr&length=4096&offset=0",
                status=200,
                payload={"data": "stderr-output from task", "offset": 0},
            )
            task_output_data = await self.mesos_client.get_task_output_data(
                self.agent_addr, task_info
            )

            self.assertEqual("stdout-output from task", task_output_data.stdout)
