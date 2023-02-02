from abc import ABC, abstractmethod
from enum import IntEnum
from typing import TYPE_CHECKING

from ..pb.tendermint.abci import (
    RequestInfo, ResponseInfo, ResponseSetOption, RequestSetOption, RequestDeliverTx, ResponseDeliverTx,
    ResponseCheckTx, RequestCheckTx, ResponseQuery, RequestQuery, RequestCommit, ResponseCommit, RequestInitChain,
    ResponseInitChain, ResponseBeginBlock, RequestBeginBlock, RequestEndBlock, ResponseEndBlock,
    RequestListSnapshots, ResponseListSnapshots, ResponseOfferSnapshot, RequestOfferSnapshot,
    RequestLoadSnapshotChunk, ResponseLoadSnapshotChunk, RequestApplySnapshotChunk, ResponseApplySnapshotChunk
)

if TYPE_CHECKING:
    from typing import Type


class ResultCode(IntEnum):
    """ ABCI result codes enum
    """
    OK = 0
    Error = 1


class InfoHandler(ABC):
    """ ABCI handler of Info connection
    """

    @abstractmethod
    async def info(self, req: 'RequestInfo') -> 'ResponseInfo':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#info
        """

    @abstractmethod
    async def set_option(self, req: 'RequestSetOption') -> 'ResponseSetOption':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#set_option
        """

    @abstractmethod
    async def query(self, req: 'RequestQuery') -> 'ResponseQuery':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#query
        """


class MempoolHandler(ABC):
    """ ABCI handler of Mempool connection
    """

    @abstractmethod
    async def check_tx(self, req: 'RequestCheckTx') -> 'ResponseCheckTx':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#check_tx
        """


class ConsensusHandler(ABC):
    """ ABCI handler of Consensus connection
    """

    @abstractmethod
    async def init_chain(self, req: 'RequestInitChain') -> 'ResponseInitChain':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#init_chain
        """

    @abstractmethod
    async def begin_block(self, req: 'RequestBeginBlock') -> 'ResponseBeginBlock':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#begin_block
        """

    @abstractmethod
    async def deliver_tx(self, req: 'RequestDeliverTx') -> 'ResponseDeliverTx':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#deliver_tx
        """

    @abstractmethod
    async def end_block(self, req: 'RequestEndBlock') -> 'ResponseEndBlock':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#end_block
        """

    @abstractmethod
    async def commit(self, req: 'RequestCommit') -> 'ResponseCommit':
        """
        See Also: https://github.com/tendermint/tendermint/blob/main/spec/abci/abci.md#commit
        """


class StateSyncHandler(ABC):
    """ ABCI handler of StateSync connection
    """

    @abstractmethod
    async def list_snapshots(self, req: 'RequestListSnapshots') -> 'ResponseListSnapshots':
        """"""

    @abstractmethod
    async def offer_snapshot(self, req: 'RequestOfferSnapshot') -> 'ResponseOfferSnapshot':
        """"""

    @abstractmethod
    async def load_snapshot_chunk(self, req: 'RequestLoadSnapshotChunk') -> 'ResponseLoadSnapshotChunk':
        """"""

    @abstractmethod
    async def apply_snapshot_chunk(self, req: 'RequestApplySnapshotChunk') -> 'ResponseApplySnapshotChunk':
        """"""


if TYPE_CHECKING:
    HandlersKind = Type[InfoHandler] | Type[MempoolHandler] | Type[ConsensusHandler] | Type[StateSyncHandler]
    OneOfHandlers = InfoHandler | MempoolHandler | ConsensusHandler | StateSyncHandler


class HasHandlers(ABC):
    """ ABCI handler getter
    """

    @abstractmethod
    async def get_connection_handler(self, kind: 'HandlersKind') -> 'OneOfHandlers':
        """ Returns instance of requested handler.
        """


class BaseApplication(HasHandlers, InfoHandler, MempoolHandler, ConsensusHandler, ABC):
    """ Abstract base class of a simple monolithic ABCI application
    """

    async def get_connection_handler(self, kind: 'HandlersKind') -> 'OneOfHandlers':
        return self
