import json
from unittest import skip

from aiohttp.client import ClientError, ClientSession
from aioresponses import aioresponses
from asynctest import TestCase
from pydantic import ValidationError

from indexer.conf import settings
from indexer.connection import HTTPConnection
from indexer.mesos.client import (
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
            self.assertEqual(expected_directory_value, task_info.directory)
            self.assertEqual(task_id.value, task_info.id)

    async def test_task_info_completed_framework_completed_executors(self):
        task_id = TaskIdSpec(value="ct:1581360780082:0:asgard-heimdall-hml:")
        expected_directory_value = "/tmp/mesos/slaves/79ad3a13-b567-4273-ac8c-30378d35a439-S6563/frameworks/4783cf15-4fb1-4c75-90fe-44eeec5258a7-0001/executors/ct:1581360780082:0:asgard-heimdall:/runs/4d70dbf3-8131-402b-a026-a2d8e7f7ae7e"
        with aioresponses() as rsps:
            rsps.get(
                f"{self.agent_addr}/state",
                status=200,
                payload=self.agent_state_dict,
            )
            task_info = await self.mesos_client.get_task_info(
                self.agent_addr, task_id
            )
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
