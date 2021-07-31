import asyncio

from fastapi import FastAPI
from fastapi.websockets import WebSocket

from .router import MessagingRouter
from .types import RouterAuthProtocol, AuthCallback

_DEFAULT_APP_ID = "dev-app"


class _DevAuthManager(RouterAuthProtocol):
    """Dummy auth manager that lets everyone in and tells nobody"""

    def check(self, _: str) -> bool:
        return True

    def add_listener(self, callback: AuthCallback) -> None:
        pass

    async def advance(self) -> None:
        pass


app = FastAPI()
_messaging_router: MessagingRouter


@app.on_event("startup")
async def _on_startup():
    global _messaging_router
    _messaging_router = MessagingRouter(_DevAuthManager())
    asyncio.get_event_loop().create_task(_messaging_router.run_heartbeating())


@app.websocket("/in")
async def _messaging_in(websocket: WebSocket):
    await _messaging_router.client_connection(websocket, None)


@app.websocket("/out")
async def _messaging_out(websocket: WebSocket):
    await _messaging_router.app_connection(websocket, _DEFAULT_APP_ID)
