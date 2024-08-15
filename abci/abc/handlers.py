from abc import ABC, abstractmethod

from abci.types import RequestCheckTx, ResponseCheckTx, RequestInfo, ResponseInfo, RequestQuery, ResponseQuery
from abci.types import (
    RequestInitChain, ResponseInitChain, ResponsePrepareProposal, RequestPrepareProposal,
    RequestProcessProposal, ResponseProcessProposal, ResponseFinalizeBlock, ResponseExtendVote,
    RequestExtendVote, RequestVerifyVoteExtension, ResponseVerifyVoteExtension,
    ResponseCommit, RequestCommit
)
from abci.types import (
    RequestListSnapshots, ResponseListSnapshots, RequestOfferSnapshot, ResponseOfferSnapshot,
    RequestLoadSnapshotChunk, ResponseLoadSnapshotChunk, RequestApplySnapshotChunk, ResponseApplySnapshotChunk
)


class InfoHandler(ABC):
    """ Info/Query connection handler
    """

    @abstractmethod
    async def info(self, req: RequestInfo) -> ResponseInfo:
        """ Used to sync CometBFT with the Application during a handshake that
        happens upon recovery, or on startup when state-sync is used.
        """

    @abstractmethod
    async def query(self, req: RequestQuery) -> ResponseQuery:
        """ This method can be used to query the Application for information
        about the application state.
        """


class MempoolHandler(ABC):
    """ Mempool connection handler
    """

    @abstractmethod
    async def check_tx(self, req: RequestCheckTx) -> ResponseCheckTx:
        """ This method allows the Application to validate transactions. """


class ConsensusHandler(ABC):
    """ Consensus connection handler
    """

    @abstractmethod
    async def init_chain(self, req: RequestInitChain) -> ResponseInitChain:
        """ This method initializes the blockchain. CometBFT calls it once upon genesis. """

    @abstractmethod
    async def prepare_proposal(self, req: RequestPrepareProposal) -> ResponsePrepareProposal:
        """ It allows the block proposer to perform application-dependent work in a block before proposing it. """

    @abstractmethod
    async def process_proposal(self, req: RequestProcessProposal) -> ResponseProcessProposal:
        """ It allows a validator to perform application-dependent work in a proposed block. """

    @abstractmethod
    async def extend_vote(self, req: RequestExtendVote) -> ResponseExtendVote:
        """ It allows applications to let their validators do more than just validate within consensus.  """

    @abstractmethod
    async def verify_vote_extension(self, req: RequestVerifyVoteExtension) -> ResponseVerifyVoteExtension:
        """  It allows validators to validate the vote extension data attached to a precommit message. """

    @abstractmethod
    async def finalize_block(self, req: ResponseFinalizeBlock) -> ResponseFinalizeBlock:
        """ It delivers a decided block to the Application. """

    @abstractmethod
    async def commit(self, req: RequestCommit) -> ResponseCommit:
        """ Instructs the Application to persist its state. """


class StateSyncHandler(ABC):
    """ State sync connection handler
    """

    @abstractmethod
    async def list_snapshots(self, req: RequestListSnapshots) -> ResponseListSnapshots:
        """ Used by nodes to discover available snapshots on peers. """

    @abstractmethod
    async def offer_snapshot(self, req: RequestOfferSnapshot) -> ResponseOfferSnapshot:
        """ When a node receives a snapshot from a peer, CometBFT uses this method
        to offer the snapshot to the Application. """

    @abstractmethod
    async def load_snapshotChunk(self, req: RequestLoadSnapshotChunk) -> ResponseLoadSnapshotChunk:
        """ Used by CometBFT to retrieve snapshot chunks from the Application to send to peers.
        """

    @abstractmethod
    async def apply_snapshot_cChunk(self, req: RequestApplySnapshotChunk) -> ResponseApplySnapshotChunk:
        """ Used by CometBFT to hand snapshot chunks to the Application.
        """
