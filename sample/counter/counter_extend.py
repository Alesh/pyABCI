import struct
from dataclasses import dataclass
from enum import IntEnum

from tend import abci
from tend.abci.extend import RequestCheckTx, ResponseCheckTx, RequestDeliverTx
from tend.pb.tendermint.abci import ResponseQuery

from tend.abci.bhasher import DummyBlockHasher


class ResultCode(IntEnum):
    OK = 0
    EncodingError = 1
    NonceError = 2


@dataclass
class AppState(abci.AppState):
    counter: int = 0


class TxChecker(abci.TxChecker):
    """ TX checker
    """
    app_state: AppState

    async def check_tx(self, req: 'RequestCheckTx') -> 'ResponseCheckTx':
        if len(req.tx) != 4:
            return ResponseCheckTx(
                code=ResultCode.EncodingError,
                log=f"Encoded value of the counter must be a four-byte hexadecimal number, like 0x00000007. But got")
        value, = struct.unpack('>L', req.tx)
        if self.options.get('serial') == 'on':
            if not value == self.app_state.counter + 1:
                return ResponseCheckTx(code=ResultCode.NonceError,
                                       log=f"Invalid counter nonce. Expected {self.app_state.counter + 1}, got {value}")
        return ResponseCheckTx(code=ResultCode.OK)


class TxKeeper(abci.TxKeeper):
    """ TX keeper
    """
    app_state: AppState

    async def deliver_tx(self, req: 'RequestDeliverTx'):
        self.app_state.counter, = struct.unpack('>L', req.tx)
        logging.info(f'Accepted TX: {req.tx.hex().upper()}')
        return await super().deliver_tx(req)


class Counter(abci.ExtApplication):
    """ Extended ABCI Application "Counter"
    """
    serial: bool = False
    app_state: AppState

    def __init__(self):

        super().__init__(TxChecker.factory(self),
                         TxKeeper.factory(self, DummyBlockHasher),
                         initial_app_state=AppState())

    def load_genesis_state(self, *args):
        super().load_genesis_state(*args)

    async def query(self, req):
        match req.path:
            case "hash":
                return ResponseQuery(code=ResultCode.OK, value=self.app_state.app_hash)
            case "counter":
                return ResponseQuery(code=ResultCode.OK, value='0x{:08X}'.format(self.app_state.counter).encode('utf8'))
            case "height":
                return ResponseQuery(code=ResultCode.OK,
                                     value='0x{:08X}'.format(self.app_state.block_height).encode('utf8'))
            case _:
                pass
        return ResponseQuery(log=f"Invalid query path. Expected `hash` or `counter`, got {req.path}")


if __name__ == '__main__':
    import logging
    import asyncio
    from tend.abci import Server

    logging.basicConfig(level=logging.INFO)
    asyncio.run(Server(Counter()).start())
