from typing import Generic, TypeVar

AsgardModel = TypeVar("AsgardModel")
ClientModel = TypeVar("ClientModel")


class ModelConverter(Generic[AsgardModel, ClientModel]):
    @staticmethod
    def to_client_model(other: AsgardModel) -> ClientModel:
        raise NotImplementedError

    @staticmethod
    def to_asgard_model(other: ClientModel) -> AsgardModel:
        raise NotImplementedError
