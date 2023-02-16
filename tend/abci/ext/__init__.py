import logging
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import TYPE_CHECKING

from .common import CommonApp, AppState
from .txchecker import TxChecker
from .txkeeper import TxKeeper
from ..handlers import HasHandlers, InfoHandler, MempoolHandler, ConsensusHandler
from ..handlers import ResponseInfo, ResponseSetOption, ResponseQuery, ResultCode
from ..handlers import RequestInfo, RequestSetOption, RequestQuery

if TYPE_CHECKING:
    from logging import Logger
    from typing import Union, Callable, Awaitable
    from ..handlers import HandlersKind, OneOfHandlers

    Checker = Union[Callable[[], Awaitable[TxChecker]], Callable[[], TxChecker], TxChecker]
    Keeper = Union[Callable[[], Awaitable[TxKeeper]], Callable[[], TxKeeper], TxKeeper]


class Application(CommonApp, HasHandlers, ABC):
    """ Base class of an extended ABCI application
    """

    def __init__(self, checker: 'Checker', keeper: 'Keeper', logger: 'Logger' = None):
        self._checker = checker
        self._keeper = keeper
        super().__init__(logger)

    async def get_connection_handler(self, kind: 'HandlersKind') -> 'OneOfHandlers':
        async def async_apply(inst):
            if isinstance(inst, (TxChecker, TxKeeper)):
                return inst
            else:
                inst = inst()
                return inst if isinstance(inst, (TxChecker, TxKeeper)) else await inst

        if not self.state:
            await self.update_app_state(await self.get_initial_app_state())
        if kind == InfoHandler:
            return self
        elif kind == MempoolHandler:
            return await async_apply(self._checker)
        elif kind == ConsensusHandler:
            return await async_apply(self._keeper)
        else:
            raise NotImplementedError(f'Handler {kind.__qualname__} is not yet implemented')

    @abstractmethod
    async def get_initial_app_state(self) -> 'AppState':
        """ Should return initial app state
        """

    async def info(self, req: 'RequestInfo') -> 'ResponseInfo':
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'info: {asdict(req)}')
        return ResponseInfo(version=req.version,
                            last_block_height=self.state.block_height,
                            last_block_app_hash=self.state.app_hash)

    async def set_option(self, req: 'RequestSetOption') -> 'ResponseSetOption':
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'set_option: {asdict(req)}')
        self.options[req.key] = req.value
        return ResponseSetOption(code=ResultCode.OK)

    async def query(self, req: 'RequestQuery') -> 'ResponseQuery':
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f'query: {asdict(req)}')
        return ResponseQuery(code=ResultCode.Error, log=f"Invalid query path")
