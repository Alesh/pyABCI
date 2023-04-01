import asyncio

import pytest

from tend.abci import Protocol
from tend.abci.handlers import ResponseInfo, RequestInfo, RequestBeginBlock, RequestDeliverTx, RequestEndBlock
from tend.pb.tendermint.abci import RequestFlush, RequestEcho, ResponseBeginBlock, ResponseDeliverTx, ResponseEndBlock
from .mocks import ServerState, MockTransport, StubApplication
from .mocks import message_to_bytes


def test_echo_flush():
    protocol = Protocol(StubApplication(), ServerState())
    transport = MockTransport()

    protocol.connection_made(transport)
    assert protocol.remote == ['0.0.0.0', '00000']

    req = RequestEcho(message='TEST')
    protocol.data_received(message_to_bytes(req))
    req = RequestFlush()
    protocol.data_received(message_to_bytes(req))

    assert transport._buffer == b'\x10\x12\x06\n\x04TEST\x04\x1a\x00'


@pytest.mark.asyncio
async def test_info_flush():
    # The Flush response must be processed after all others have been processed.

    class Application(StubApplication):
        async def info(self, req: RequestInfo):
            await asyncio.sleep(0.05)
            return ResponseInfo(version=req.version, last_block_height=0, last_block_app_hash=b'')

    protocol = Protocol(Application(), ServerState())
    transport = MockTransport()

    protocol.connection_made(transport)
    assert protocol.remote == ['0.0.0.0', '00000']

    req = RequestInfo(version='VER0')
    protocol.data_received(message_to_bytes(req))
    req = RequestFlush()
    protocol.data_received(message_to_bytes(req))
    await asyncio.sleep(0.1)
    assert transport._buffer == b'\x10"\x06\x12\x04VER0\x04\x1a\x00'


@pytest.mark.asyncio
async def test_begin_deliver_end_flush():
    # An EndBlock request must be sent to the application after all ResponseDeliverTx responses have been processed.
    # The Flush response must be sent after all others have been processed and sent.

    class Application(StubApplication):

        async def begin_block(self, req: RequestBeginBlock):
            return ResponseBeginBlock()

        async def deliver_tx(self, req: RequestDeliverTx):
            data, temp = req.tx[:3], req.tx[3:]
            timeout = int(temp) / 100
            await asyncio.sleep(timeout)
            return ResponseDeliverTx(code=1 if timeout >= 0.1 else 0, data=data)

        async def end_block(self, req: RequestEndBlock):
            return ResponseEndBlock()


    protocol = Protocol(Application(), ServerState())
    transport = MockTransport()

    protocol.connection_made(transport)
    assert protocol.remote == ['0.0.0.0', '00000']

    begin_block = RequestBeginBlock()
    protocol.data_received(message_to_bytes(begin_block))

    tx0 = RequestDeliverTx(tx=b'TX005')
    protocol.data_received(message_to_bytes(tx0))
    tx1 = RequestDeliverTx(tx=b'TX115')
    protocol.data_received(message_to_bytes(tx1))
    tx2 = RequestDeliverTx(tx=b'TX210')
    protocol.data_received(message_to_bytes(tx2))

    req = RequestEndBlock()
    protocol.data_received(message_to_bytes(req))
    req = RequestFlush()
    protocol.data_received(message_to_bytes(req))

    await asyncio.sleep(0.2)
    assert transport._buffer == b'\x04B\x00\x0eR\x05\x12\x03TX0\x12R\x07\x08\x01\x12\x03TX2\x12R\x07\x08\x01\x12\x03TX1\x04Z\x00\x04\x1a\x00'