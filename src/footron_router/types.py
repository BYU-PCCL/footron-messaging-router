from abc import abstractmethod
from datetime import datetime
from typing import Protocol, Callable, Union, Awaitable, Dict, Any

import footron_protocol as protocol

JsonDict = Dict[str, Union[Any, Any]]
DisplaySettingsCallback = Callable[[protocol.DisplaySettings], None]
InteractionCallback = Callable[[datetime], None]
AuthCallback = Callable[[str], Union[Awaitable[None], None]]


class RouterAuthProtocol(Protocol):
    @abstractmethod
    def check(self, auth_code: str) -> bool:
        ...

    @abstractmethod
    def check_next(self, auth_code: str) -> bool:
        ...

    @abstractmethod
    async def advance(self) -> None:
        ...

    @abstractmethod
    def add_listener(self, callback: AuthCallback) -> None:
        ...
