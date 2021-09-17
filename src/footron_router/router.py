from __future__ import annotations

import asyncio
import dataclasses
import datetime
import logging
import uuid
from typing import Dict, Union, TYPE_CHECKING, List, Set, Optional

import footron_protocol as protocol
from fastapi import WebSocket
from starlette.concurrency import run_until_first_complete
from starlette.websockets import WebSocketState
from websockets.exceptions import ConnectionClosed

from .util import asyncio_interval

if TYPE_CHECKING:
    from .types import RouterAuthProtocol, JsonDict, DisplaySettingsCallback, \
    InteractionCallback

logger = logging.getLogger(__name__)


# TODO: Move this somewhere
async def _checked_socket_send(message: JsonDict, socket: WebSocket) -> bool:
    if socket.application_state == WebSocketState.DISCONNECTED:
        return False

    try:
        await socket.send_json(message)
    except (RuntimeError, ConnectionClosed) as e:
        logger.error(f"Error during socket send: {e}")
        return False
    return True


async def _checked_socket_close(socket: WebSocket) -> bool:
    if socket.application_state == WebSocketState.DISCONNECTED:
        return False

    try:
        await socket.close()
    except (RuntimeError, ConnectionClosed) as e:
        logger.error(f"Error during socket close: {e}")
        return False
    return True


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
    queue: asyncio.Queue[protocol.BaseMessage] = dataclasses.field(
        default_factory=asyncio.Queue
    )
    last_client_message_time: datetime.datetime = None
    clients: Dict[str, _ClientConnection] = dataclasses.field(default_factory=dict)
    closed = False
    lock: protocol.Lock = False
    lock_last_update: Optional[datetime.datetime] = None

    async def send_message_from_client(
        self, client_id: str, message: protocol.BaseMessage
    ):
        if not issubclass(message.__class__, protocol.AppClientIdentifiableMixin):
            raise protocol.ProtocolError(
                f"Client {client_id} attempted to send message type with no 'client' field"
            )
        message.client = client_id
        self.last_client_message_time = datetime.datetime.now()
        return await self.queue.put(message)

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

    async def connect_existing_client(self, client: _ClientConnection):
        # Send directly to the socket instead of queuing to avoid the side effect of
        # automatically sending an access message back to the client in
        # _handle_send_message.
        # TODO: Determine if this ugliness points to a need for a better abstraction
        await _checked_socket_send(
            protocol.serialize(protocol.ConnectMessage(client=client.id)), self.socket
        )
        self.clients[client.id] = client

    async def remove_client(self, client_id: str):
        """Notify app that a client has been disconnected."""
        if client_id not in self.clients:
            return

        self.clients[client_id].app_id = None
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
            if not self.router.client_connected(message.client):
                # This should always be an developer error, but if it isn't (e.g.
                # we're sending positive heartbeats that include disconnected
                # clients), we need to fix our code
                logger.warning(
                    f"App {self.id} attempted to send a message to non-existent client with id {message.client}"
                )

                # Correct if client is somehow leftover in internal state
                if self.has_client(message.client):
                    await self.remove_client(message.client)

                await self.send_heartbeat(message.client, False)
                return

            client = self.router.clients[message.client]

            if isinstance(message, protocol.AccessMessage):
                if not self.lock:
                    await self.queue.put(
                        protocol.ErrorMessage(
                            error="A lock is required to send access responses"
                        )
                    )
                    return

                await self.add_client(
                    client
                ) if message.accepted else await self.remove_client(message.client)

                await client.send_message_from_app(self.id, message)
                return

            if not self.has_client(message.client):
                logger.warning(
                    f"App {self.id} attempted to send application-level message to unauthenticated client with id {message.client}"
                )
                await self.send_heartbeat(message.client, False)
                return

            return await self._send_to_client(message)

        if isinstance(message, protocol.DisplaySettingsMessage):
            if message.settings.lock:
                self.last_lock_update = datetime.datetime.now()
                self.lock = message.settings.lock
            return self.display_settings_callback(message.settings)

        raise protocol.UnhandledMessageTypeError(
            f"Unable to handle message type '{message.type}' from app '{self.id}'"
        )

    async def _handle_connect_message(self, message: protocol.ConnectMessage):
        try:
            client = self.router.clients[message.client]
        except KeyError:
            logging.error(
                f"Client '{message.client}' sent a connect message to app '{self.id}', but isn't connected"
            )
            return

        if self.lock is True or (
            isinstance(self.lock, int)
            and not isinstance(self.lock, bool)
            and len(self.clients) >= self.lock
        ):
            # TODO: Come up with a less terse message (eventually the user might
            #  see these)
            await client.deauth("A session lock is in effect", app_id=self.id)
            return

        await self.add_client(client)
        await client.send_access_message(True, app_id=self.id)

    async def _handle_send_message(self, message: protocol.BaseMessage):
        if isinstance(message, protocol.ConnectMessage):
            await self._handle_connect_message(message)

        return await _checked_socket_send(protocol.serialize(message), self.socket)

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

        try:
            await self.clients[message.client].send_message_from_app(self.id, message)
        except KeyError:
            raise ValueError(
                f"App '{self.id}' attempted to send message to unregistered client '{message.client}'"
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
    queue: asyncio.Queue[protocol.BaseMessage] = dataclasses.field(
        default_factory=asyncio.Queue
    )
    closed = False

    async def _send_or_disconnect(self, message: JsonDict):
        if self.closed:
            return False

        if not await _checked_socket_send(message, self.socket):
            await self.close()

    async def send_message_from_app(self, app_id: str, message: protocol.BaseMessage):
        if not issubclass(message.__class__, protocol.AppClientIdentifiableMixin):
            raise protocol.ProtocolError(
                f"App {app_id} attempted to send message type with no 'app' field"
            )
        message.app = app_id
        return await self.queue.put(message)

    async def send_access_message(
        self, accepted: bool, *, reason: str = None, app_id: str = None
    ):
        message = protocol.AccessMessage(accepted=accepted, reason=reason, app=app_id)
        return await self.queue.put(message)

    async def deauth(
        self,
        reason="Your authentication code is expired or invalid",
        app_id: str = None,
    ):
        return await self.send_access_message(False, reason=reason, app_id=app_id)

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

    async def _handle_send_message(self, message: protocol.BaseMessage):
        """Return false to cancel connection, true to continue without sending"""

        if self.closed:
            return False

        if not self._pre_send(message):
            return True

        serialized_message = protocol.serialize(message)
        # Client doesn't need to know its ID because it doesn't have to self-identify
        try:
            del serialized_message["client"]
        except KeyError:
            pass
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
    _auth: RouterAuthProtocol
    apps: Dict[str, _AppConnection]
    # dict[connection id, connection]--multiple clients are only allowed when
    # either a lock is specified or multiuser is true in app config
    clients: Dict[str, _ClientConnection]
    _display_settings_listeners: Set[DisplaySettingsCallback]
    interaction_listeners: Set[InteractionCallback]

    def __init__(self, auth: RouterAuthProtocol):
        self.apps = {}
        self.clients = {}
        self._auth = auth
        self._display_settings_listeners = set()
        self.interaction_listeners = set()

        self._auth.add_listener(self._disconnect_deauthed_clients)

    async def run_heartbeating(self):
        await asyncio_interval(self._send_heartbeats, 0.5)

    async def app_connection(self, socket: WebSocket, app_id):
        connection = _AppConnection(
            socket, app_id, self._notify_display_settings_listeners, self
        )

        await self._connect_app(connection)

        if connection.socket.application_state != WebSocketState.CONNECTED:
            await self._disconnect_app(connection)
            return
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

        if connection.socket.application_state != WebSocketState.CONNECTED:
            await self._disconnect_client(connection, deauth=False)
            return
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
        await self._prune_clients()

        # Only one client should stay connected through an app transition--if multiple
        # are connected then we have a race condition or a bug in our state management
        # somewhere
        if len(self.clients):
            await connection.connect_existing_client(next(iter(self.clients.values())))

    async def _disconnect_app(self, connection: _AppConnection):
        await connection.close()

        if connection.id not in self.apps:
            return

        await self._prune_clients()

        del self.apps[connection.id]

    async def _prune_clients(self):
        # If multiple clients are connected, deauth them all. We do this because
        # connecting multiple clients requires an app-controlled interaction lock that
        # will prevent anyone not in the group of connected clients from scanning in.

        # @vinhowe: The rationale here is that it seems fair to kick everyone in a group
        # with exclusive control so they can't monopolize the display; If we let any
        # member of a lock group stay connected, they could instantly jump into another
        # locked session. We really don't want people to feel like they can't interact
        # with the display because someone else is "using it," and I figure making users
        # scan in again makes it a little more obvious that the display is meant to be
        # a public installation and not a game console.
        #
        # It's totally possible that this isn't enough. We want the ability to highlight
        # the game development work done in our department, and I think that a short
        # multiplayer game session created by a student here could inspire someone to
        # get into computer science. But if we find that groups are preventing others
        # from interacting with the display, we will need to either come up with some
        # other mitigation (like cooldowns for some types of experiences) or remove the
        # lock API and force developers using it to retool their apps.
        if len(self.clients) <= 1:
            return

        await self._auth.advance()
        await asyncio.gather(
            *[self._disconnect_client(client) for client in list(self.clients.values())]
        )

    async def _try_connect_client(self, connection: _ClientConnection):
        await connection.connect()

        if not self._auth.check(connection.auth_code) and not self._auth.check_next(
            connection.auth_code
        ):
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
        self._display_settings_listeners.add(callback)

    def remove_display_settings_listener(self, callback: DisplaySettingsCallback):
        self._display_settings_listeners.remove(callback)

    def _notify_display_settings_listeners(self, settings: protocol.DisplaySettings):
        [callback(settings) for callback in self._display_settings_listeners]

    def add_interaction_listener(self, callback: InteractionCallback):
        self.interaction_listeners.add(callback)

    def remove_interaction_listener(self, callback: InteractionCallback):
        self.interaction_listeners.remove(callback)

    def _notify_interaction_listeners(self, at: datetime):
        [callback(at) for callback in self.interaction_listeners]

    async def _send_heartbeats(self):
        """Send heartbeats to all connected clients and apps"""
        tasks = []
        latest_message_time = datetime.datetime.fromtimestamp(0)
        any_lock = False
        for app in self.apps.values():
            if (
                app.last_client_message_time
                and app.last_client_message_time > latest_message_time
            ):
                latest_message_time = app.last_client_message_time
            if app.lock is True or (app.lock is False and app.lock_last_update):
                any_lock = True
            tasks.append(app.send_client_heartbeats())

        now = datetime.datetime.now()

        # Notify interaction listeners if we've received a message in the last 30s AND
        # no connected apps either
        # - have a closed lock
        # - have no lock AND have a set lock_last_update field, meaning they were locked
        #   at one point and are now unlocked, which means they're waiting to be cleaned
        #   up by the timer
        # This second point could cause us problems if we're not careful about
        # sequencing and handling edge cases.
        message_time_delta = now - latest_message_time
        if (
            not any_lock
            and message_time_delta.days == 0
            and message_time_delta.seconds == 0
        ):
            self._notify_interaction_listeners(now)

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
                    lambda c: not self._auth.check(c.auth_code),
                    list(self.clients.values()),
                ),
            )
        )
