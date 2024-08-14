import struct
from enum import IntEnum

from abci import BaseApplication
from abci.ext.bhasher import BlockHasher, DummyBlockHasher
from abci.pb.tendermint.abci import (
    RequestInfo, ResponseInfo, ResponseInitChain, ResponseCheckTx, RequestCheckTx, RequestDeliverTx,
    ResponseDeliverTx, RequestQuery, ResponseQuery, ResponseCommit, ResponseBeginBlock, ResponseEndBlock,
    RequestSetOption, ResponseSetOption
)


class ResultCode(IntEnum):
    OK = 0
    EncodingError = 1
    NonceError = 2


class Counter(BaseApplication):
    """ ABCI Application "Counter"
    """
    counter: int = 0
    block_height: int = 0
    app_hash: bytes = b''
    serial = False

    block_hasher: BlockHasher

    async def set_option(self, req: RequestSetOption):
        self.serial = req.key == 'serial' and req.value == 'on'
        return ResponseSetOption(code=ResultCode.OK)

    async def info(self, req: RequestInfo):
        return ResponseInfo(version=req.version,
                            last_block_height=self.block_height,
                            last_block_app_hash=self.app_hash)

    async def init_chain(self, _):
        return ResponseInitChain()

    async def begin_block(self, _):
        self.block_hasher = DummyBlockHasher()
        return ResponseBeginBlock()

    async def check_tx(self, req: RequestCheckTx) -> ResponseCheckTx:
        if len(req.tx) != 4:
            return ResponseCheckTx(
                code=ResultCode.EncodingError,
                log=f"Encoded value of the counter must be a four-byte hexadecimal number, like 0x00000007. But got")
        value, = struct.unpack('>L', req.tx)
        if self.serial:
            if not value == self.counter + 1:
                return ResponseCheckTx(code=ResultCode.NonceError,
                                       log=f"Invalid counter nonce. Expected {self.counter + 1}, got {value}")
        return ResponseCheckTx(code=ResultCode.OK)

    async def deliver_tx(self, req: RequestDeliverTx):
        self.block_hasher.write_tx(req.tx)
        self.counter, = struct.unpack('>L', req.tx)
        logging.info(f'Accepted TX: {req.tx.hex().upper()}')
        return ResponseDeliverTx(code=ResultCode.OK)

    async def end_block(self, req):
        self.block_height = req.height
        return ResponseEndBlock()

    async def commit(self, _):
        self.app_hash = self.block_hasher.sum()
        return ResponseCommit(data=self.app_hash)

    async def query(self, req: RequestQuery):
        match req.path:
            case "hash":
                return ResponseQuery(code=ResultCode.OK, value=self.app_hash)
            case "counter":
                return ResponseQuery(code=ResultCode.OK, value='0x{:08X}'.format(self.counter).encode('utf8'))
            case "height":
                return ResponseQuery(code=ResultCode.OK, value='0x{:08X}'.format(self.block_height).encode('utf8'))
            case _:
                pass
        return ResponseQuery(log=f"Invalid query path. Expected `hash` or `counter`, got {req.path}")


if __name__ == '__main__':
    import logging
    import asyncio
    from abci import Server

    logging.basicConfig(level=logging.INFO)
    asyncio.run(Server(Counter()).start())
