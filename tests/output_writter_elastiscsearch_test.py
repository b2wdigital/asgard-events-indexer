import asyncio
import json

from freezegun import freeze_time

from indexer.conf import settings
from indexer.connection import HTTPConnection
from indexer.mesos.models.converters.taskupdated import (
    MesosTaskUpdatedEventConverter,
)
from indexer.mesos.models.event import MesosEvent
from indexer.models.event import Event
from indexer.writter import ElasticSearchOutputWritter
from tests.base import BaseTestCase, FIXTURE_DIR


class ElasticSearchOutputWritterTest(BaseTestCase):
    async def setUp(self):
        self.es_out_writter = ElasticSearchOutputWritter(
            HTTPConnection(urls=settings.ES_OUTPUT_URLS)
        )
        self.index_name_prefix = "asgard-events-2020-01-23-17*"
        await self.es_out_writter.client.indices.delete(
            self.index_name_prefix, allow_no_indices=True
        )

    async def test_instantiate_es_client_with_hosts_list(self):
        self.assertEqual(
            [{"host": "127.0.0.1", "port": 9200}],
            self.es_out_writter.client.transport.hosts,
        )

    @freeze_time("2020-01-23T17:23:43.451742+00:00")
    async def test_index_one_event_using_the_correct_index_name(self):
        mesos_task_updated_data = open(
            f"{FIXTURE_DIR}/mesos_state_running_event_data.json"
        ).read()

        mesos_event = MesosEvent(**json.loads(mesos_task_updated_data))
        asgard_event = MesosTaskUpdatedEventConverter.to_asgard_model(
            mesos_event.task_updated
        )
        await self.es_out_writter.write([asgard_event])

        await asyncio.sleep(1)

        result = await self.es_out_writter.client.search(
            index=self.index_name_prefix, body={}
        )
        self.assertEqual(1, result["hits"]["total"])

        doc_data = result["hits"]["hits"][0]["_source"]
        asgard_saved_event = Event(**doc_data)
        self.assertEqual(asgard_saved_event.dict(), asgard_event.dict())

    @freeze_time("2020-01-19T13:23:43.451742+00:00")
    async def test_generate_index_prefix(self):
        index_name = self.es_out_writter._get_index_name()
        self.assertEqual(index_name, "asgard-events-2020-01-19-13")
