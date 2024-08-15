import hashlib
import logging
from abc import ABC
from typing import Union, Callable

from abci import abc
from abci.abc import ConsensusHandler, InfoHandler, MempoolHandler, StateSyncHandler
from abci.types import (
    RequestQuery, ResponseQuery, RequestCheckTx, ResponseCheckTx, RequestInitChain, ResponseInitChain,
    RequestPrepareProposal, ResponsePrepareProposal, ResponseProcessProposal, RequestProcessProposal, ProposalStatus,
    RequestExtendVote, ResponseExtendVote, RequestVerifyVoteExtension, ResponseVerifyVoteExtension, VerifyStatus,
    RequestFinalizeBlock, ResponseFinalizeBlock, ExecTxResult, RequestApplySnapshotChunk, ResponseApplySnapshotChunk,
    RequestLoadSnapshotChunk, ResponseLoadSnapshotChunk, RequestOfferSnapshot, ResponseOfferSnapshot,
    RequestListSnapshots, ResponseListSnapshots
)


class SimpleApp(abc.Application, ConsensusHandler, InfoHandler, MempoolHandler, StateSyncHandler, ABC):
    """ Simple application that accept all TX from network """

    def __init__(self, on_block_height: Callable[[int], None], logger: logging.Logger = None):
        self.__logger = logger or logging.root
        self.on_block_height = on_block_height

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    @property
    def handlers(self) -> Union[ConsensusHandler, InfoHandler, MempoolHandler, StateSyncHandler]:
        return self

    async def init_chain(self, req: RequestInitChain) -> ResponseInitChain:
        self.on_block_height(req.initial_height)
        return ResponseInitChain(app_hash=hashlib.sha256(req.app_state_bytes).digest())

    async def query(self, req: RequestQuery) -> ResponseQuery:
        if req.path == '/store' and req.data:
            return ResponseQuery(log="key does not exist", key=req.data)
        return ResponseQuery(log="Query unrecognized")

    async def check_tx(self, req: RequestCheckTx) -> ResponseCheckTx:
        return ResponseCheckTx()  # all accepts

    async def prepare_proposal(self, req: RequestPrepareProposal) -> ResponsePrepareProposal:
        return ResponsePrepareProposal(txs=req.txs)  # all accepts

    async def process_proposal(self, req: RequestProcessProposal) -> ResponseProcessProposal:
        return ResponseProcessProposal(status=ProposalStatus.ACCEPT)  # all accepts

    async def extend_vote(self, req: RequestExtendVote) -> ResponseExtendVote:
        return ResponseExtendVote()  # all accepts

    async def verify_vote_extension(self, req: RequestVerifyVoteExtension) -> ResponseVerifyVoteExtension:
        return ResponseVerifyVoteExtension(status=VerifyStatus.ACCEPT)  # all accepts

    async def finalize_block(self, req: RequestFinalizeBlock) -> ResponseFinalizeBlock:
        self.on_block_height(req.height)
        return ResponseFinalizeBlock(tx_results=[ExecTxResult() for _ in req.txs])

    async def list_snapshots(self, req: RequestListSnapshots) -> ResponseListSnapshots:
        return ResponseListSnapshots()

    async def offer_snapshot(self, req: RequestOfferSnapshot) -> ResponseOfferSnapshot:
        return ResponseOfferSnapshot()

    async def load_snapshotChunk(self, req: RequestLoadSnapshotChunk) -> ResponseLoadSnapshotChunk:
        return ResponseLoadSnapshotChunk()

    async def apply_snapshot_cChunk(self, req: RequestApplySnapshotChunk) -> ResponseApplySnapshotChunk:
        return ResponseApplySnapshotChunk()
