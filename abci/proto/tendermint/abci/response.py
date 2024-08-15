from dataclasses import dataclass, field
from typing import Optional

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from pure_protobuf.one_of import OneOf
from typing_extensions import Annotated

from abci.proto.tendermint.abci.enums import ProposalStatus, VerifyStatus, OfferSnapshotResult, ApplySnapshotChunkResult
from abci.proto.tendermint.abci.types import ValidatorUpdate, Event, ExecTxResult, Snapshot
from abci.proto.tendermint.crypto import ProofOps
from abci.proto.tendermint.types import ConsensusParams


@dataclass
class ResponseException(BaseMessage):
    error: Annotated[str, Field(1)] = ""


@dataclass
class ResponseEcho(BaseMessage):
    message: Annotated[str, Field(1)] = ""


@dataclass
class ResponseFlush(BaseMessage):
    pass


@dataclass
class ResponseInfo(BaseMessage):
    data: Annotated[str, Field(1)] = ""
    version: Annotated[str, Field(2)] = ""
    app_version: Annotated[int, Field(3)] = 0
    last_block_height: Annotated[int, Field(4)] = 0
    last_block_app_hash: Annotated[bytes, Field(5)] = b""


@dataclass
class ResponseQuery(BaseMessage):
    code: Annotated[int, Field(1)] = 0
    log: Annotated[str, Field(3)] = None
    info: Annotated[str, Field(4)] = None
    index: Annotated[int, Field(5)] = 0
    key: Annotated[bytes, Field(6)] = None
    value: Annotated[bytes, Field(7)] = None
    proof_ops: Annotated[ProofOps, Field(8)] = None
    height: Annotated[int, Field(9)] = 0
    codespace: Annotated[str, Field(10)] = None


@dataclass
class ResponseCheckTx(BaseMessage):
    code: Annotated[int, Field(1)] = 0
    data: Annotated[bytes, Field(2)] = b""
    log: Annotated[str, Field(3)] = ""
    info: Annotated[str, Field(4)] = ""
    gas_wanted: Annotated[int, Field(5)] = 0
    gas_used: Annotated[int, Field(6)] = 0
    events: Annotated[list[Event], Field(7)] = field(default_factory=list)
    codespace: Annotated[str, Field(8)] = ""


@dataclass
class ResponseInitChain(BaseMessage):
    consensus_params: Annotated[ConsensusParams, Field(1)] = None
    validators: Annotated[list[ValidatorUpdate], Field(2)] = None
    app_hash: Annotated[bytes, Field(3)] = b""


@dataclass
class ResponsePrepareProposal(BaseMessage):
    txs: Annotated[list[bytes], Field(1)] = field(default_factory=list)


@dataclass
class ResponseProcessProposal(BaseMessage):
    status: Annotated[ProposalStatus, Field(1)] = ProposalStatus.UNKNOWN


@dataclass
class ResponseExtendVote(BaseMessage):
    vote_extension: Annotated[bytes, Field(1)] = b""


@dataclass
class ResponseVerifyVoteExtension(BaseMessage):
    status: Annotated[VerifyStatus, Field(1)] = VerifyStatus.UNKNOWN


@dataclass
class ResponseCommit(BaseMessage):
    retain_height: Annotated[int, Field(3)] = 0


@dataclass
class ResponseFinalizeBlock(BaseMessage):
    events: Annotated[list[Event], Field(1)] = field(default_factory=list)
    tx_results: Annotated[list[ExecTxResult], Field(2)] = field(default_factory=list)
    validator_updates: Annotated[list[ValidatorUpdate], Field(3)] = field(default_factory=list)
    consensus_param_updates: Annotated[ConsensusParams, Field(4)] = None
    app_hash: Annotated[bytes, Field(5)] = b""


@dataclass
class ResponseListSnapshots(BaseMessage):
    snapshots: Annotated[list[Snapshot], Field(1)] = field(default_factory=list)


@dataclass
class ResponseOfferSnapshot(BaseMessage):
    result: Annotated[OfferSnapshotResult, Field(1)] = OfferSnapshotResult.UNKNOWN


@dataclass
class ResponseLoadSnapshotChunk(BaseMessage):
    chunk: Annotated[bytes, Field(1)] = b""


@dataclass
class ResponseApplySnapshotChunk(BaseMessage):
    result: Annotated[ApplySnapshotChunkResult, Field(1)] = ApplySnapshotChunkResult.UNKNOWN
    refetch_chunks: Annotated[list[int], Field(2)] = field(default_factory=list)
    reject_senders: Annotated[list[str], Field(3)] = field(default_factory=list)


@dataclass
class Response(BaseMessage):
    """ Response types.py """
    value = OneOf[Optional[BaseMessage]]()
    which_one = value.which_one_of_getter()

    exception: Annotated[Optional[ResponseException], Field(1, one_of=value)] = None
    echo: Annotated[Optional[ResponseEcho], Field(2, one_of=value)] = None
    flush: Annotated[Optional[ResponseFlush], Field(3, one_of=value)] = None
    info: Annotated[Optional[ResponseInfo], Field(4, one_of=value)] = None
    init_chain: Annotated[Optional[ResponseInitChain], Field(6, one_of=value)] = None
    query: Annotated[Optional[ResponseQuery], Field(7, one_of=value)] = None
    check_tx: Annotated[Optional[ResponseCheckTx], Field(9, one_of=value)] = None
    commit: Annotated[Optional[ResponseCommit], Field(12, one_of=value)] = None
    list_snapshots: Annotated[Optional[ResponseListSnapshots], Field(13, one_of=value)] = None
    offer_snapshot: Annotated[Optional[ResponseOfferSnapshot], Field(14, one_of=value)] = None
    load_snapshot_chunk: Annotated[Optional[ResponseLoadSnapshotChunk], Field(15, one_of=value)] = None
    apply_snapshot_chunk: Annotated[Optional[ResponseApplySnapshotChunk], Field(16, one_of=value)] = None
    prepare_proposal: Annotated[Optional[ResponsePrepareProposal], Field(17, one_of=value)] = None
    process_proposal: Annotated[Optional[ResponseProcessProposal], Field(18, one_of=value)] = None
    extend_vote: Annotated[Optional[ResponseExtendVote], Field(19, one_of=value)] = None
    verify_vote_extension: Annotated[Optional[ResponseVerifyVoteExtension], Field(20, one_of=value)] = None
    finalize_block: Annotated[Optional[ResponseFinalizeBlock], Field(21, one_of=value)] = None
