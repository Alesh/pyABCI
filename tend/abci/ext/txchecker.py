import logging
from abc import abstractmethod
from dataclasses import asdict
from typing import TYPE_CHECKING

from ..handlers import MempoolHandler, ResultCode, ResponseCheckTx
from .common import HasRelatedApp, CommonApp

if TYPE_CHECKING:
    from ..handlers import RequestCheckTx


class TxChecker(MempoolHandler, HasRelatedApp):
    """ Transaction checker
    """

    def __init__(self, app: 'CommonApp'):
        self.__app = app

    @property
    def app(self) -> 'CommonApp':
        return self.__app

    @abstractmethod
    async def check_tx(self, req: 'RequestCheckTx') -> 'ResponseCheckTx':
        if self.app.logger.isEnabledFor(logging.DEBUG):
            self.app.logger.debug(f'check_tx: {asdict(req)}')
        return ResponseCheckTx(code=ResultCode.OK)
