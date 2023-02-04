import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import TYPE_CHECKING

from .bhasher import BlockHasher, DummyBlockHasher

from .handlers import (
    MempoolHandler, InfoHandler, ConsensusHandler, HasHandlers, ResultCode,

    ResponseInitChain, ResponseBeginBlock, ResponseEndBlock, ResponseCommit, ResponseDeliverTx, RequestInfo,
    ResponseCheckTx, ResponseSetOption, ResponseQuery,

    RequestInitChain, RequestBeginBlock, RequestEndBlock, RequestCommit, RequestDeliverTx, ResponseInfo,
    RequestCheckTx, RequestSetOption, RequestQuery
)

if TYPE_CHECKING:
    from logging import Logger
    from typing import Callable, Coroutine, Any, Union
    from .handlers import OneOfHandlers, HandlersKind

    Options = dict[str, 'Any']


@dataclass
class AppState:
    """ Base ABCI application state ""

    Attributes:
        block_height: block height in chain
        app_hash: Hash of the application state
    """
    block_height: int = 0
    app_hash: bytes = b''


def clone_app_state(app_state: 'AppState') -> 'AppState':
    return type(app_state)(**asdict(app_state))


class HasAppState(ABC):
    """ Has application state
    """

    @property
    @abstractmethod
    def app_state(self) -> 'AppState':
        """ Returns application state
        """


class HasAppOptions(ABC):
    """ Has application options
    """

    @property
    @abstractmethod
    def options(self) -> 'Options':
        """ Returns application options
        """


class HasAppLogger(ABC):
    """ Has application logger
    """

    @property
    @abstractmethod
    def logger(self) -> 'Logger':
        """ Returns application logger
        """


if TYPE_CHECKING:
    App = Union['HasAppState', 'HasAppOptions']


class TxChecker(HasAppState, HasAppOptions, HasAppLogger, MempoolHandler, ABC):
    """ Transaction checker
    """

    def __init__(self, app: 'App', logger: 'Logger' = None):
        self.__app = app
        self.__logger = logger or logging.root

    @property
    def app_state(self) -> 'AppState':
        return self.__app.app_state

    @property
    def options(self) -> 'Options':
        return self.__app.options

    @property
    def logger(self) -> 'Logger':
        return self.__logger

    @abstractmethod
    async def check_tx(self, req: 'RequestCheckTx') -> 'ResponseCheckTx':
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'check_tx: {asdict(req)}')

    @classmethod
    async def factory(cls, has: 'HasAppState', logger: 'Logger' = None):
        return cls(has, logger)


class HasMutableAppState(HasAppState):
    """ Mutable application state
    """

    @abstractmethod
    async def update_app_state(self, new_app_state: 'AppState'):
        """ Updates app_state
        """

    @abstractmethod
    async def load_genesis_state(self, genesis_data: bytes):
        """ Updates app_state
        """


if TYPE_CHECKING:
    App = Union['HasMutableAppState', 'HasAppOptions']


class TxKeeper(HasAppState, HasAppOptions, HasAppLogger, ConsensusHandler):
    """ Transaction keeper
    """

    block_hasher: BlockHasher

    def __init__(self, app: 'App', block_hasher_factory: 'BlockHasherFactory', logger: 'Logger' = None):
        self._app = app
        self._block_hasher_factory = block_hasher_factory
        self.__app_state = clone_app_state(app.app_state)
        self.__logger = logger or logging.root

    @property
    def app_state(self) -> 'AppState':
        return self.__app_state

    @property
    def options(self) -> 'Options':
        return self._app.options

    @property
    def logger(self) -> 'Logger':
        return self.__logger

    async def init_chain(self, req: 'RequestInitChain'):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'init_chain: {asdict(req)}')
        if req.app_state_bytes:
            await self._app.load_genesis_state(req.app_state_bytes)
        return ResponseInitChain()

    async def begin_block(self, req: 'RequestBeginBlock'):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'begin_block: {asdict(req)}')
        self.block_hasher = self._block_hasher_factory()
        return ResponseBeginBlock()

    async def deliver_tx(self, req: 'RequestDeliverTx'):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'deliver_tx: {asdict(req)}')
        self.block_hasher.write_tx(req.tx)
        return ResponseDeliverTx(code=ResultCode.OK)

    async def end_block(self, req: 'RequestEndBlock'):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'end_block: {asdict(req)}')
        self.app_state.block_height = req.height
        return ResponseEndBlock()

    async def commit(self, req: 'RequestCommit') -> 'ResponseCommit':
        self.app_state.app_hash = self.block_hasher.sum()
        new_app_state = clone_app_state(self.app_state)
        await self._app.update_app_state(new_app_state)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'commit. app_state: {asdict(new_app_state)}')
        return ResponseCommit(self.app_state.app_hash)

    @classmethod
    async def factory(cls, hmas: 'HasMutableAppState', bhf: 'BlockHasherFactory', logger: 'Logger' = None):
        return cls(hmas, bhf, logger)


if TYPE_CHECKING:
    BlockHasherFactory = Callable[[], BlockHasher]
    AsyncTxCheckerFactory = Coroutine[Any, Any, TxChecker]
    AsyncTxKeeperFactory = Coroutine[Any, Any, TxChecker]


class ExtApplication(HasMutableAppState, HasAppLogger, HasAppOptions, HasHandlers, InfoHandler):
    """ Abstract base class of an extended ABCI application
    """

    def __init__(self, async_tx_checker_factory: 'AsyncTxCheckerFactory',
                 async_tx_keeper_factory: 'AsyncTxKeeperFactory' = None, logger: 'Logger' = None):
        self.__app_state = None
        self.__logger = logger or logging.root
        self._async_tx_checker_factory = async_tx_checker_factory
        self._async_tx_keeper_factory = async_tx_keeper_factory or TxKeeper.factory(self, DummyBlockHasher, logger)
        self.__options = dict()

    @property
    def app_state(self) -> 'AppState':
        return self.__app_state

    @property
    def options(self) -> 'Options':
        return self.__options

    @property
    def logger(self) -> 'Logger':
        return self.__logger

    async def update_app_state(self, new_app_state: 'AppState'):
        self.__app_state = new_app_state

    def load_genesis_state(self, *args):
        raise RuntimeError(f'`{self.__class__.__qualname__}`.load_genesis_state is not implemented,'
                           + ' but `genesis.app_state` has received.')

    async def get_connection_handler(self, kind: 'HandlersKind') -> 'OneOfHandlers':
        match kind.__qualname__:
            case InfoHandler.__qualname__:
                return self
            case MempoolHandler.__qualname__:
                return await self._async_tx_checker_factory
            case ConsensusHandler.__qualname__:
                return await self._async_tx_keeper_factory
            case _:
                raise NotImplementedError(f'Handler {kind.__qualname__} is not yet implemented')

    async def info(self, req: 'RequestInfo'):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'info: {asdict(req)}')
        if not self.app_state:
            await self.update_app_state(AppState())
        return ResponseInfo(version=req.version,
                            last_block_height=self.app_state.block_height,
                            last_block_app_hash=self.app_state.app_hash)

    async def set_option(self, req: 'RequestSetOption') -> 'ResponseSetOption':
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'set_option: {asdict(req)}')
        self.__options[req.key] = req.value
        return ResponseSetOption(code=ResultCode.OK)

    async def query(self, req: 'RequestQuery') -> 'ResponseQuery':
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'query: {asdict(req)}')
        return ResponseQuery(code=ResultCode.Error, log=f"Invalid query path")
