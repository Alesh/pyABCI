import asyncio
import re

import betterproto

import abci
from abci.protocol import ServerState, Protocol
from abci.handlers import (
    RequestCommit
)
from abci.pb.tendermint.abci import Request, Response


class MockTransport(asyncio.Transport):
    """ Mock transport
    """

    def __init__(self):
        super().__init__()
        self._buffer = b''

    def get_extra_info(self, name, **kwargs):
        return ['0.0.0.0', '00000'] if name == 'peername' else None

    def write(self, data: bytes):
        self._buffer += data


class MockServerState(ServerState):
    """ Mock server state
    """
    connections: set['Protocol'] = set()

    async def stop(self):
        pass


class StubApplication(abci.BaseApplication):
    """ Stub application
    """

    async def info(self, req): pass

    async def init_chain(self, req): pass

    async def set_option(self, req): pass

    async def query(self, req): pass

    async def check_tx(self, req): pass

    async def begin_block(self, req): pass

    async def deliver_tx(self, req): pass

    async def end_block(self, req): pass

    async def commit(self, req: 'RequestCommit'): pass


def message_to_bytes(message: betterproto.Message) -> bytes:
    if message.__class__.__name__.startswith('Request'):
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', message.__class__.__name__[7:]).lower()
        message = Request(**{name: message})
    elif message.__class__.__name__.startswith('Response'):
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', message.__class__.__name__[7:]).lower()
        message = Response(**{name: message})
    else:
        raise ValueError
    data = message.SerializeToString()
    return betterproto.encode_varint(len(data) << 1) + data
