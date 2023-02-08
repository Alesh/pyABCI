import logging
from abc import abstractmethod
from dataclasses import asdict
from typing import TYPE_CHECKING

from .bhasher import BlockHasher, DummyBlockHasher
from ..handlers import ConsensusHandler, ResultCode
from ..handlers import ResponseInitChain, ResponseBeginBlock, ResponseDeliverTx, ResponseEndBlock, ResponseCommit
from .common import HasRelatedApp, CommonApp, HasAppState, AppState

if TYPE_CHECKING:
    from typing import Callable
    from ..handlers import RequestInitChain, RequestBeginBlock, RequestDeliverTx, RequestEndBlock, RequestCommit

    BlockHasherFactory = Callable[[], BlockHasher]


class TxKeeper(ConsensusHandler, HasRelatedApp, HasAppState):
    """ Transaction keeper
    """

    def __init__(self, app: 'CommonApp', block_hasher_factory: 'BlockHasherFactory' = None):
        self.__app = app
        self.__bh = self.__state = None
        self.__bhf = block_hasher_factory or DummyBlockHasher

    @property
    def app(self) -> 'CommonApp':
        return self.__app

    @property
    def state(self) -> 'AppState':
        return self.__state

    async def init_chain(self, req: 'RequestInitChain') -> 'ResponseInitChain':
        """ Init chain ABCI handler """
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'init_chain: {asdict(req)}')
        if req.app_state_bytes:
            await self.app.load_genesis(req.app_state_bytes)
        return ResponseInitChain()

    async def begin_block(self, req: 'RequestBeginBlock') -> 'ResponseBeginBlock':
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'begin_block: {asdict(req)}')
        self.__state = self.app.state.clone()
        self.__bh = self.__bhf()
        return ResponseBeginBlock()

    @abstractmethod
    async def deliver_tx(self, req: 'RequestDeliverTx') -> 'ResponseDeliverTx':
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'deliver_tx: {asdict(req)}')
        self.__bh.write_tx(req.tx)
        return ResponseDeliverTx(code=ResultCode.OK)

    async def end_block(self, req: 'RequestEndBlock') -> 'ResponseEndBlock':
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'end_block: {asdict(req)}')
        self.state.block_height = req.height
        return ResponseEndBlock()

    async def commit(self, req: 'RequestCommit') -> 'ResponseCommit':
        self.state.app_hash = self.__bh.sum()
        await self.app.update_app_state(self.state)
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'commit. app_state: {asdict(self.state)}')
        return ResponseCommit(self.state.app_hash)
