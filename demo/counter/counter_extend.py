import struct
from dataclasses import dataclass
from enum import IntEnum

from abci.ext.bhasher import DummyBlockHasher
from abci.abc.handlers import RequestCheckTx, ResponseCheckTx, RequestDeliverTx
from abci.pb.tendermint.abci import ResponseQuery

from abci import ext as abci


class ResultCode(IntEnum):
    OK = 0
    EncodingError = 1
    NonceError = 2
    QueryError = 3


@dataclass
class AppState(abci.AppState):
    counter: int = 0


class TxChecker(abci.TxChecker):
    """ TX checker
    """
    app: 'Counter'

    async def check_tx(self, req: 'RequestCheckTx') -> 'ResponseCheckTx':
        if len(req.tx) != 4:
            return ResponseCheckTx(
                code=ResultCode.EncodingError,
                log=f"Encoded value of the counter must be a four-byte hexadecimal number, like 0x00000007. But got")
        value, = struct.unpack('>L', req.tx)
        if self.app.options.get('serial') == 'on':
            if not value == self.app.state.counter + 1:
                return ResponseCheckTx(code=ResultCode.NonceError,
                                       log=f"Invalid counter nonce. Expected {self.app.state.counter + 1}, got {value}")
        return ResponseCheckTx(code=ResultCode.OK)


class TxKeeper(abci.TxKeeper):
    """ TX keeper
    """
    app: 'Counter'

    async def deliver_tx(self, req: 'RequestDeliverTx'):
        self.app.state.counter, = struct.unpack('>L', req.tx)
        logging.info(f'Accepted TX: {req.tx.hex().upper()}')
        return await super().deliver_tx(req)


class Counter(abci.Application):
    """ Extended ABCI Application "Counter"
    """

    state: AppState
    serial: bool = False

    def __init__(self):
        super().__init__(TxChecker(self), TxKeeper(self, DummyBlockHasher))

    async def get_initial_app_state(self):
        return AppState()

    async def query(self, req):
        match req.path:
            case "hash":
                return ResponseQuery(code=ResultCode.OK, value=self.state.app_hash)
            case "counter":
                return ResponseQuery(code=ResultCode.OK, value='0x{:08X}'.format(self.state.counter).encode('utf8'))
            case "height":
                return ResponseQuery(code=ResultCode.OK,
                                     value='0x{:08X}'.format(self.state.block_height).encode('utf8'))
            case _:
                pass
        return ResponseQuery(code=ResultCode.QueryError, log=f"Invalid query path. Expected `hash` or `counter`, got {req.path}")


if __name__ == '__main__':
    import logging
    import asyncio
    from abci import Server

    logging.basicConfig(level=logging.INFO)
    asyncio.run(Server(Counter()).start())
