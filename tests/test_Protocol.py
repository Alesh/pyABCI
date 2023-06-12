import asyncio

import pytest

from abci import Protocol
from abci.handlers import ResponseInfo, RequestInfo, RequestBeginBlock, RequestDeliverTx, RequestEndBlock
from abci.pb.tendermint.abci import RequestFlush, RequestEcho, ResponseBeginBlock, ResponseDeliverTx, ResponseEndBlock, \
    RequestCommit, RequestCheckTx
from tests.mocks import MockServerState, MockTransport, StubApplication
from tests.mocks import message_to_bytes


@pytest.mark.asyncio
async def test_echo_flush():
    protocol = Protocol(StubApplication(), MockServerState())
    transport = MockTransport()

    protocol.connection_made(transport)
    assert protocol.remote == ['0.0.0.0', '00000']

    req = RequestEcho(message='TEST')
    protocol.data_received(message_to_bytes(req))
    req = RequestFlush()
    protocol.data_received(message_to_bytes(req))
    await asyncio.sleep(0.1)
    assert transport._buffer == b'\x10\x12\x06\n\x04TEST\x04\x1a\x00'


@pytest.mark.asyncio
async def test_info_flush():
    # The Flush response must be processed after all others have been processed.

    class Application(StubApplication):
        async def info(self, req: RequestInfo):
            await asyncio.sleep(0.05)
            return ResponseInfo(version=req.version, last_block_height=0, last_block_app_hash=b'')

    protocol = Protocol(Application(), MockServerState())
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
async def test_consensus_handler():
    # Checks determined (ordered) execution for ConsensusHandler begin_block > deliver_tx... > end_block > commit

    class Application(StubApplication):
        result = []

        async def begin_block(self, req: RequestBeginBlock):
            return ResponseBeginBlock()

        async def deliver_tx(self, req: RequestDeliverTx):
            data, temp = req.tx[:3], req.tx[3:]
            timeout = int(temp) / 100
            await asyncio.sleep(timeout)
            self.result.append(data)
            return ResponseDeliverTx(code=1 if timeout >= 0.1 else 0, data=data)

        async def end_block(self, req: RequestEndBlock):
            return ResponseEndBlock()

    app = Application()
    protocol = Protocol(app, MockServerState())
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
    req = RequestCommit()
    protocol.data_received(message_to_bytes(req))
    req = RequestFlush()
    protocol.data_received(message_to_bytes(req))

    await asyncio.sleep(0.5)
    assert transport._buffer == b'\x04B\x00\x0eR\x05\x12\x03TX0\x12R\x07\x08\x01\x12\x03TX1\x12R\x07\x08\x01\x12\x03TX2\x04Z\x00\x00\x04\x1a\x00'
    assert tuple(app.result) == (b'TX0', b'TX1', b'TX2')


@pytest.mark.asyncio
async def test_mempool_handler():
    # Checks determined (ordered) execution for MempoolHandler

    class Application(StubApplication):
        result = []

        async def check_tx(self, req: RequestDeliverTx):
            data, temp = req.tx[:3], req.tx[3:]
            timeout = int(temp) / 100
            await asyncio.sleep(timeout)
            self.result.append(data)
            return ResponseDeliverTx(code=1 if timeout >= 0.1 else 0, data=data)

    app = Application()
    protocol = Protocol(app, MockServerState())
    transport = MockTransport()

    protocol.connection_made(transport)
    assert protocol.remote == ['0.0.0.0', '00000']

    tx0 = RequestCheckTx(tx=b'TX005')
    protocol.data_received(message_to_bytes(tx0))
    tx1 = RequestCheckTx(tx=b'TX115')
    protocol.data_received(message_to_bytes(tx1))
    tx2 = RequestCheckTx(tx=b'TX210')
    protocol.data_received(message_to_bytes(tx2))

    req = RequestFlush()
    protocol.data_received(message_to_bytes(req))

    await asyncio.sleep(0.5)
    assert transport._buffer == b'\x0eJ\x05\x12\x03TX0\x12J\x07\x08\x01\x12\x03TX1\x12J\x07\x08\x01\x12\x03TX2\x04\x1a\x00'
    assert tuple(app.result) == (b'TX0', b'TX2', b'TX1')
