from typing import (
    Tuple,
)

from lahja import (
    BaseEvent,
    ConnectionConfig,
)


class ShutdownRequest(BaseEvent):

    def __init__(self, reason: str="") -> None:
        self.reason = reason


class EventBusConnected(BaseEvent):
    """
    Broadcasted when a new :class:`~lahja.endpoint.Endpoint` connects to the ``main``
    :class:`~lahja.endpoint.Endpoint`.
    """

    def __init__(self, connection_config: ConnectionConfig) -> None:
        self.connection_config = connection_config


class AvailableEndpointsUpdated(BaseEvent):
    """
    Broadcasted by the ``main`` :class:`~lahja.endpoint.Endpoint` after it has received a
    :class:`~trinity.events.EventBusConnected` event. The ``available_endpoints`` property
    lists all available endpoints that are known at the time when the event is raised.
    """

    def __init__(self, available_endpoints: Tuple[ConnectionConfig, ...]) -> None:
        self.available_endpoints = available_endpoints
