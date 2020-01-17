from indexer.mesos.models.converters.spec import MesosEventSourceSpecConverter
from indexer.mesos.models.spec import MesosEventSourceSpec
from indexer.models.event import EventSourceSpec
from tests.base import BaseTestCase


class MesosEventSourceSpecConverterTest(BaseTestCase):
    async def test_convert_source_executor(self):
        self.assertEqual(
            EventSourceSpec.SOURCE_EXECUTOR,
            MesosEventSourceSpecConverter.to_asgard_model(
                MesosEventSourceSpec.SOURCE_EXECUTOR
            ),
        )

    async def test_convert_source_master(self):
        self.assertEqual(
            EventSourceSpec.SOURCE_MASTER,
            MesosEventSourceSpecConverter.to_asgard_model(
                MesosEventSourceSpec.SOURCE_MASTER
            ),
        )

    async def test_convert_source_agent(self):
        self.assertEqual(
            EventSourceSpec.SOURCE_AGENT,
            MesosEventSourceSpecConverter.to_asgard_model(
                MesosEventSourceSpec.SOURCE_AGENT
            ),
        )
