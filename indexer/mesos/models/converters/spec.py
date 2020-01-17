from indexer.mesos.models.spec import MesosEventSourceSpec
from indexer.mesos.models.spec.taskdata import MesosTaskDataSpec
from indexer.models.converter import ModelConverter
from indexer.models.event import (
    EventSourceSpec,
    ContainerInfoSpec,
    ContainerInfoResourcesSpec,
    ContainerInfoVolumeItemSpec,
    ContainerInfoLabelsItemSpec,
)

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


class MesosTaskDataSpecConverter(
    ModelConverter[ContainerInfoSpec, MesosTaskDataSpec]
):
    @staticmethod
    def to_asgard_model(other: MesosTaskDataSpec) -> ContainerInfoSpec:
        return ContainerInfoSpec(
            name=other.Name,
            pid=other.State.Pid,
            running=other.State.Running,
            exit_code=other.State.ExitCode,
            error=other.State.Error,
            hostname=other.config.Hostname,
            image=other.config.Image,
            resources=ContainerInfoResourcesSpec(
                cpu_shares=other.HostConfig.CpuShares,
                cpu_quota=other.HostConfig.CpuQuota,
                memory_swap=other.HostConfig.MemorySwap,
                memory_swappiness=other.HostConfig.MemorySwappiness,
            ),
            volumes=[
                ContainerInfoVolumeItemSpec(
                    host_path=volume_info_item.Source,
                    container_path=volume_info_item.Destination,
                    mode=volume_info_item.Mode,
                )
                for volume_info_item in other.Mounts
            ],
            labels=[
                ContainerInfoLabelsItemSpec(name=label_name, value=label_value)
                for label_name, label_value in other.config.Labels.items()
            ],
        )
