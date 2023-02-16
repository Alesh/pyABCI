import logging
from abc import abstractmethod
from dataclasses import asdict
from typing import TYPE_CHECKING

from .bhasher import BlockHasher, DummyBlockHasher
from .common import HasRelatedApp, CommonApp, AppState
from ..handlers import ConsensusHandler, ResultCode
from ..handlers import RequestInitChain, RequestBeginBlock, RequestDeliverTx, RequestEndBlock, RequestCommit
from ..handlers import ResponseInitChain, ResponseBeginBlock, ResponseDeliverTx, ResponseEndBlock, ResponseCommit

if TYPE_CHECKING:
    from typing import Callable
    BlockHasherFactory = Callable[[], BlockHasher]


class TxKeeper(ConsensusHandler, HasRelatedApp):
    """ Transaction keeper
    """
    block_height: int
    block_hasher: BlockHasher

    def __init__(self, app: 'CommonApp', block_hasher_factory: 'BlockHasherFactory' = None):
        self.__app = app
        self.__block_hasher_factory = block_hasher_factory or DummyBlockHasher
        self.block_hasher = self.__block_hasher_factory()

    @property
    def app(self) -> 'CommonApp':
        return self.__app

    async def load_genesis(self, genesis_data: bytes):
        raise RuntimeError(f'`{self.__class__.__qualname__}`.load_genesis_state is not implemented,'
                           + ' but `genesis.app_state` has received.')

    async def init_chain(self, req: 'RequestInitChain') -> 'ResponseInitChain':
        """ Init chain ABCI handler """
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'init_chain: {asdict(req)}')
        app_hash = b''
        if req.app_state_bytes:
            app_hash = await self.load_genesis(req.app_state_bytes)
            self.block_hasher.write_hash(app_hash)
        return ResponseInitChain(app_hash=app_hash)

    async def begin_block(self, req: 'RequestBeginBlock') -> 'ResponseBeginBlock':
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'begin_block: {asdict(req)}')
        return ResponseBeginBlock()

    @abstractmethod
    async def deliver_tx(self, req: 'RequestDeliverTx') -> 'ResponseDeliverTx':
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'deliver_tx: {asdict(req)}')
        self.block_hasher.write_tx(req.tx)
        return ResponseDeliverTx(code=ResultCode.OK)

    async def end_block(self, req: 'RequestEndBlock') -> 'ResponseEndBlock':
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'end_block: {asdict(req)}')
        self.block_height = req.height
        return ResponseEndBlock()

    async def commit(self, req: 'RequestCommit') -> 'ResponseCommit':
        app_state = AppState(block_height=self.block_height, app_hash=self.block_hasher.sum())
        self.block_hasher = self.__block_hasher_factory()
        await self.app.update_app_state(app_state)
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'commit. app_state: {asdict(app_state)}')
        return ResponseCommit(app_state.app_hash)
