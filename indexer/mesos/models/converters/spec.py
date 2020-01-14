from indexer.mesos.models.spec import MesosEventSourceSpec
from indexer.models.converter import ModelConverter
from indexer.models.event import EventSourceSpec

lookup_table = {
    MesosEventSourceSpec.SOURCE_AGENT: EventSourceSpec.SOURCE_AGENT,
    MesosEventSourceSpec.SOURCE_EXECUTOR: EventSourceSpec.SOURCE_EXECUTOR,
    MesosEventSourceSpec.SOURCE_MASTER: EventSourceSpec.SOURCE_MASTER,
}


class MesosEventSourceSpecConverter(
    ModelConverter[EventSourceSpec, MesosEventSourceSpec]
):
    @staticmethod
    def to_asgard_model(other: MesosEventSourceSpec) -> EventSourceSpec:
        return lookup_table[other]
