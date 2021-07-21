from __future__ import annotations

import asyncio
import dataclasses
import logging
import uuid
from typing import Dict, Union, TYPE_CHECKING, List, Set, Optional

import footron_protocol as protocol
from fastapi import WebSocket
from starlette.concurrency import run_until_first_complete
from starlette.websockets import WebSocketState
from websockets.exceptions import ConnectionClosedError

from .util import asyncio_interval

if TYPE_CHECKING:
    from .types import RouterAuthProtocol, JsonDict, DisplaySettingsCallback

logger = logging.getLogger(__name__)


# TODO: Move this somewhere
async def _checked_socket_send(message: JsonDict, socket: WebSocket) -> bool:
    if socket.application_state == WebSocketState.DISCONNECTED:
        return False

    try:
        await socket.send_json(message)
    except (RuntimeError, ConnectionClosedError) as e:
        logger.error(f"Error during socket send: {e}")
        return False
    return True


async def _checked_socket_close(socket: WebSocket) -> bool:
    if socket.application_state == WebSocketState.DISCONNECTED:
        return False

    try:
        await socket.close()
    except (RuntimeError, ConnectionClosedError) as e:
        logger.error(f"Error during socket close: {e}")
        return False
    return True


@dataclasses.dataclass
class _AppBoundMessageInfo:
    client: str
    message: protocol.BaseMessage


@dataclasses.dataclass
class _ClientBoundMessageInfo:
    app: str
    message: protocol.BaseMessage


# TODO: Consider if AppConnection and ClientConnection are similar enough that they
#  can share logic
@dataclasses.dataclass
class _AppConnection:
    socket: WebSocket
    id: str
    display_settings_callback: DisplaySettingsCallback
    # TODO: I don't love how we're passing in an instance of the containing class
    #  here, is this clean?
    router: MessagingRouter
    queue: asyncio.Queue[
        Union[protocol.BaseMessage, _AppBoundMessageInfo]
    ] = asyncio.Queue()
    clients: Dict[str, _ClientConnection] = dataclasses.field(default_factory=dict)
    closed = False
    lock: protocol.Lock = False

    async def send_message_from_client(
        self, client_id: str, message: protocol.BaseMessage
    ):
        return await self.queue.put(_AppBoundMessageInfo(client_id, message))

    async def connect(self):
        return await self.socket.accept()

    async def close(self) -> bool:
        if self.closed:
            return False
        await _checked_socket_close(self.socket)
        self.closed = True
        return True

    async def send_heartbeat(self, clients: Union[str, List[str]], up: bool):
        # Note that an "up" heartbeat containing a list of clients is expected to be
        # comprehensive, and any clients not listed should be removed. Likewise,
        # a "down" heartbeat containing a list of clients should be interpreted as a
        # list of clients to remove.

        if isinstance(clients, str):
            clients = [clients]

        # TODO: This is probably too verbose, consider removing it
        logger.debug(f"Sending heartbeat to app: {self.id}")
        return await _checked_socket_send(
            protocol.serialize(protocol.HeartbeatClientMessage(up=up, clients=clients)),
            self.socket,
        )

    async def send_client_heartbeats(self):
        return await self.send_heartbeat(list(self.clients.keys()), True)

    async def remove_client(self, client_id: str):
        """Notify app that a client has been disconnected."""
        if client_id not in self.clients:
            return

        del self.clients[client_id]

        return await self.send_heartbeat(client_id, False)

    async def add_client(self, client: _ClientConnection):
        # TODO: Should we throw an exception here if a client already exists in the
        #  mapping with that ID? Or is just overwriting it (like we do now) a
        #  reasonable default?

        self.clients[client.id] = client

        # Send first heartbeat including new client
        return await self.send_client_heartbeats()

    def has_client(self, client_id: str):
        return client_id in self.clients

    async def receive_handler(self):
        """Handle messages from socket: app -> client"""
        async for message in self.socket.iter_json():
            await self._handle_receive_message(protocol.deserialize(message))

    async def send_handler(self):
        """Handle messages in queue: client -> app"""
        while True:
            if not await self._handle_send_message(await self.queue.get()):
                return

    async def _handle_receive_message(self, message: protocol.BaseMessage):
        if hasattr(message, "client"):
            # TODO: Assert that these two statements are always equal
            if not self.has_client(message.client) or not self.router.client_connected(
                message.client
            ):
                # This should always be an developer error, but if it isn't (e.g.
                # we're sending positive heartbeats that include disconnected
                # clients), we need to fix our code
                logger.warning(
                    f"App {self.id} attempted to send a message to non-existent client with id {message.client}"
                )
                await self.send_heartbeat(message.client, False)
                return

            if isinstance(message, protocol.AccessMessage):
                self.add_client(
                    self.router.clients[message.client]
                ) if message.accepted else self.remove_client(message.client)

            return await self._send_to_client(message)

        if isinstance(message, protocol.DisplaySettingsMessage):
            return self.display_settings_callback(message.settings)

        raise protocol.UnhandledMessageTypeError(
            f"Unable to handle message type '{message.type}' from app '{self.id}'"
        )

    async def _handle_send_message(
        self, item: Union[protocol.BaseMessage, _AppBoundMessageInfo]
    ):
        message = None
        if isinstance(item, _AppBoundMessageInfo):

            # TODO (ASAP): Finish defining lock logic--for example, we need to make sure
            #  not to kick an existing client off when a new client scans a new lock
            #  auth code--which sounds just a little tricky
            if isinstance(item.message, protocol.ConnectMessage) and not self.lock:
                await self.add_client(self.router.clients[item.client])
                await self._send_to_client(
                    protocol.AccessMessage(
                        app=self.id, client=item.client, accepted=True
                    )
                )

            message = protocol.serialize(item.message)
            # App needs to know source of client messages
            message["client"] = item.client
        if isinstance(item, protocol.BaseMessage):
            message = protocol.serialize(item)

        if message is None:
            raise TypeError("Message wasn't _AppBoundMessageInfo or BaseMessage")

        return await _checked_socket_send(message, self.socket)

    async def _send_to_client(
        self, message: Union[protocol.BaseMessage, protocol.AppClientIdentifiableMixin]
    ):
        # TODO: The type hint for 'message' feels incorrect as it appears to
        #  represent either an instance of a BaseMessage or a
        #  AppClientIdentifiableMixin, but not a combination of both
        if not hasattr(message, "client"):
            raise ValueError(
                f"App '{self.id}' attempted to send message to client without specifying client ID"
            )

        return await self.clients[message.client].send_message_from_app(
            self.id, message
        )


@dataclasses.dataclass
class _ClientConnection:
    socket: WebSocket
    id: str
    auth_code: Optional[str]
    # TODO: I don't love how we're passing in an instance of the containing class
    #  here, is this clean?
    router: MessagingRouter
    # While this is None, no app has accepted this client connection
    app_id: str = None
    queue: asyncio.Queue[
        Union[protocol.BaseMessage, _ClientBoundMessageInfo]
    ] = asyncio.Queue()
    closed = False

    async def _send_or_disconnect(self, message: JsonDict):
        if self.closed:
            return False

        if not await _checked_socket_send(message, self.socket):
            await self.close()

    async def send_message_from_app(self, app_id: str, message: protocol.BaseMessage):
        return await self.queue.put(_ClientBoundMessageInfo(app_id, message))

    async def send_access_message(
        self, accepted: bool, *, reason: str = None, app_id: str = None
    ):
        # This needs to be sent immediately
        await self._send_or_disconnect(
            protocol.serialize(
                protocol.AccessMessage(accepted=accepted, reason=reason, app=app_id)
            ),
        )

    async def deauth(self, reason="Your authentication code is expired or invalid"):
        return await self.send_access_message(False, reason=reason)

    async def send_heartbeat(self, app: str, up: bool):
        logger.debug(f"Sending heartbeat to client: {self.id}")
        await self._send_or_disconnect(
            protocol.serialize(protocol.HeartbeatAppMessage(app=app, up=up)),
        )

    async def connect(self):
        return await self.socket.accept()

    async def close(self) -> bool:
        if self.closed:
            return False
        await _checked_socket_close(self.socket)

        if self.app_id and self.app_id in self.router.apps:
            await self.router.apps[self.app_id].remove_client(self.id)

        self.closed = True
        return True

    async def receive_handler(self):
        """Handle messages from socket: client -> app"""
        async for message in self.socket.iter_json():
            await self._handle_receive_message(message)

    async def send_handler(self):
        """Handle messages in queue: app -> client"""
        while True:
            if not await self._handle_send_message(await self.queue.get()):
                return

    async def _handle_receive_message(self, data: JsonDict):
        message = protocol.deserialize(data)

        app_id = (
            message.app if isinstance(message, protocol.ConnectMessage) else self.app_id
        )

        if not app_id:
            raise protocol.AccessError(
                f"Sending message type '{message.type}' before authentication is not allowed"
            )

        if isinstance(
            message, (protocol.HeartbeatAppMessage, protocol.HeartbeatClientMessage)
        ):
            # Heartbeat messages should be created by router only based on status of
            # websocket connections
            raise protocol.ProtocolError(
                "Clients are not allowed to send heartbeat messages"
            )

        if isinstance(message, protocol.ErrorMessage):
            raise protocol.ProtocolError(
                "Clients are not allowed to send protocol-level error messages"
            )

        if not isinstance(
            message,
            (
                protocol.ConnectMessage,
                protocol.LifecycleMessage,
                protocol.ApplicationClientMessage,
            ),
        ):
            raise protocol.ProtocolError(
                f"Clients are not allowed to send message type '{message.type}'"
            )

        if not self.router.app_connected(app_id):
            return await self.send_heartbeat(app_id, False)

        return await self.router.apps[app_id].send_message_from_client(self.id, message)

    async def _handle_send_message(
        self, item: Union[protocol.BaseMessage, _ClientBoundMessageInfo]
    ):
        """Return false to cancel connection, true to continue without sending"""

        if self.closed:
            return False

        message = None
        serialized_message = None

        if isinstance(item, _ClientBoundMessageInfo):
            if not self._pre_send(item.message):
                return True
            message = item.message
            serialized_message = protocol.serialize(message)
            # Client needs to know source of app messages
            serialized_message["app"] = item.app
        if isinstance(item, protocol.BaseMessage):
            if not self._pre_send(item):
                return True
            message = item
            serialized_message = protocol.serialize(message)

        if message is None:
            raise TypeError("Message wasn't _AppBoundMessageInfo or BaseMessage")

        # Client doesn't need to know its ID because it doesn't have to self-identify
        del serialized_message["client"]
        await self._send_or_disconnect(serialized_message)

        if not self._post_send(message):
            # Cancel connection
            return False

        return True

    def _pre_send(self, message: protocol.BaseMessage) -> bool:
        """This function is only intended to prevent misguided apps from sending
        application messages to unauthenticated clients, and its result should never
        be used to close a client connection--that responsibility falls to
        _post_send()"""
        if isinstance(message, protocol.AccessMessage):
            # An access message must always be sent--by this point we've already
            # prevented the app from sending its own access messages without a lock
            if message.accepted:
                self.app_id = message.app
            return True

        if self.app_id:
            return True

        # Defensively prevent apps from sending messages to clients they haven't
        # accepted--this should be handled in app messaging libraries, but this helps
        # us cover our bases
        return False

    @staticmethod
    def _post_send(message: protocol.BaseMessage):
        if isinstance(message, protocol.AccessMessage) and not message.accepted:
            return False

        return True


class MessagingRouter:
    auth: RouterAuthProtocol
    apps: Dict[str, _AppConnection]
    # dict[connection id, connection]--multiple clients are only allowed when
    # either a lock is specified or multiuser is true in app config
    clients: Dict[str, _ClientConnection]
    display_settings_listeners: Set[DisplaySettingsCallback]

    def __init__(self, auth: RouterAuthProtocol):
        self.auth = auth
        self.apps = {}
        self.clients = {}
        self.display_settings_listeners = set()

        self.auth.add_listener(self._disconnect_deauthed_clients)

    async def run_heartbeating(self):
        await asyncio_interval(self._send_heartbeats, 0.5)

    async def app_connection(self, socket: WebSocket, app_id):
        connection = _AppConnection(
            socket, app_id, self._notify_display_settings_listeners, self
        )

        await self._connect_app(connection)
        try:
            await run_until_first_complete(
                (connection.receive_handler, {}),
                (connection.send_handler, {}),
            )
        finally:
            await self._disconnect_app(connection)

    async def client_connection(self, socket: WebSocket, auth_code: Optional[str]):
        connection = _ClientConnection(socket, str(uuid.uuid4()), auth_code, self)

        await self._try_connect_client(connection)
        try:
            await run_until_first_complete(
                (connection.receive_handler, {}),
                (connection.send_handler, {}),
            )
        finally:
            await self._disconnect_client(connection, deauth=False)

    async def _connect_app(self, connection: _AppConnection):
        await connection.connect()
        self.apps[connection.id] = connection

    async def _disconnect_app(self, connection: _AppConnection):
        await connection.close()

        if connection.id not in self.apps:
            return

        del self.apps[connection.id]

    async def _try_connect_client(self, connection: _ClientConnection):
        await connection.connect()

        if not self.auth.check(connection.auth_code):
            await connection.deauth()
            await connection.close()
            return

        self.clients[connection.id] = connection

    async def _disconnect_client(self, connection: _ClientConnection, deauth=True):
        # @vinhowe: If deauth is false, well-behaved clients may try to reconnect
        # with the same auth information. I'm not sure whether there's actually a
        # scenario where we don't want to deauth a client on disconnect because
        # client sessions are now scoped
        if deauth:
            await connection.deauth()

        await connection.close()

        if connection.id not in self.clients:
            return

        del self.clients[connection.id]

        # Let app know client has disconnected
        if connection.app_id and self.app_connected(connection.app_id):
            # TODO: Determine if we should be using a higher level API here,
            #  e.g. something like .disconnect_client() instead of .send_heartbeat()
            await self.apps[connection.app_id].remove_client(connection.id)

    def app_connected(self, app_id: str) -> bool:
        return app_id in self.apps

    def client_connected(self, client_id: str) -> bool:
        return client_id in self.clients

    def add_display_settings_listener(self, callback: DisplaySettingsCallback):
        self.display_settings_listeners.add(callback)

    def remove_display_settings_listener(self, callback: DisplaySettingsCallback):
        self.display_settings_listeners.remove(callback)

    def _notify_display_settings_listeners(self, settings: protocol):
        [callback(settings) for callback in self.display_settings_listeners]

    async def _send_heartbeats(self):
        """Send heartbeats to all connected clients and apps"""
        tasks = []
        for app in self.apps.values():
            tasks.append(app.send_client_heartbeats())

        for client in self.clients.values():
            if not client.app_id:
                continue

            tasks.append(
                client.send_heartbeat(client.app_id, self.app_connected(client.app_id))
            )

        await asyncio.gather(*tasks)

    async def _disconnect_deauthed_clients(self, _new_code):
        return await asyncio.gather(
            *map(
                self._disconnect_client,
                filter(
                    lambda c: not self.auth.check(c.auth_code), self.clients.values()
                ),
            )
        )
