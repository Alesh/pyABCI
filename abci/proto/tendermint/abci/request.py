from dataclasses import dataclass, field
from typing import Optional

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from pure_protobuf.one_of import OneOf
from typing_extensions import Annotated

from abci.proto.google.protobuf import Timestamp
from abci.proto.tendermint.abci.enums import CheckTxType
from abci.proto.tendermint.abci.types import ValidatorUpdate, ExtendedCommitInfo, Misbehavior, CommitInfo, Snapshot
from abci.proto.tendermint.types import ConsensusParams


@dataclass
class RequestEcho(BaseMessage):
    message: Annotated[str, Field(1)]


@dataclass
class RequestFlush(BaseMessage):
    pass


@dataclass
class RequestInfo(BaseMessage):
    version: Annotated[str, Field(1)]
    block_version: Annotated[int, Field(2)]
    p2p_version: Annotated[int, Field(3)]
    abci_version: Annotated[str, Field(4)]


@dataclass
class RequestQuery(BaseMessage):
    data: Annotated[bytes, Field(1)]
    path: Annotated[str, Field(2)] = '/store'
    height: Annotated[int, Field(3)] = 0
    prove: Annotated[bool, Field(4)] = False


@dataclass
class RequestCheckTx(BaseMessage):
    tx: Annotated[bytes, Field(1)]
    type: Annotated[CheckTxType, Field(2)] = CheckTxType.NEW


@dataclass
class RequestInitChain(BaseMessage):
    time: Annotated[Timestamp, Field(1)]
    chain_id: Annotated[str, Field(2)]
    consensus_params: Annotated[ConsensusParams, Field(3)]
    validators: Annotated[list[ValidatorUpdate], Field(4)]
    app_state_bytes: Annotated[bytes, Field(5)] = b''
    initial_height: Annotated[int, Field(6)] = 0


@dataclass
class RequestPrepareProposal(BaseMessage):
    max_tx_bytes: Annotated[int, Field(1)]
    local_last_commit: Annotated[ExtendedCommitInfo, Field(3)]
    height: Annotated[int, Field(5)]
    time: Annotated[Timestamp, Field(6)]
    next_validators_hash: Annotated[bytes, Field(7)]
    proposer_address: Annotated[bytes, Field(8)]
    txs: Annotated[list[bytes], Field(2)] = field(default_factory=list)
    misbehavior: Annotated[list[Misbehavior], Field(4)] = field(default_factory=list)


@dataclass
class RequestProcessProposal(BaseMessage):
    proposed_last_commit: Annotated[CommitInfo, Field(2)]
    hash: Annotated[bytes, Field(4)]
    height: Annotated[int, Field(5)]
    time: Annotated[Timestamp, Field(6)]
    next_validators_hash: Annotated[bytes, Field(7)]
    proposer_address: Annotated[bytes, Field(8)]
    txs: Annotated[list[bytes], Field(1)] = field(default_factory=list)
    misbehavior: Annotated[list[Misbehavior], Field(3)] = field(default_factory=list)


@dataclass
class RequestExtendVote(BaseMessage):
    hash: Annotated[bytes, Field(1)]
    height: Annotated[int, Field(2)]
    time: Annotated[Timestamp, Field(3)]
    proposed_last_commit: Annotated[CommitInfo, Field(5)]
    next_validators_hash: Annotated[bytes, Field(7)]
    proposer_address: Annotated[bytes, Field(8)]
    txs: Annotated[list[bytes], Field(4)] = field(default_factory=list)
    misbehavior: Annotated[list[Misbehavior], Field(6)] = field(default_factory=list)


@dataclass
class RequestVerifyVoteExtension(BaseMessage):
    hash: Annotated[bytes, Field(1)]
    validator_address: Annotated[bytes, Field(2)]
    height: Annotated[int, Field(3)]
    vote_extension: Annotated[bytes, Field(4)]


@dataclass
class RequestCommit(BaseMessage):
    pass


@dataclass
class RequestFinalizeBlock(BaseMessage):
    decided_last_commit: Annotated[CommitInfo, Field(2)]
    hash: Annotated[bytes, Field(4)]
    height: Annotated[int, Field(5)]
    time: Annotated[Timestamp, Field(6)]
    next_validators_hash: Annotated[bytes, Field(7)]
    proposer_address: Annotated[bytes, Field(8)]
    txs: Annotated[list[bytes], Field(1)] = field(default_factory=list)
    misbehavior: Annotated[list[Misbehavior], Field(3)] = field(default_factory=list)


@dataclass
class RequestListSnapshots(BaseMessage):
    pass


@dataclass
class RequestOfferSnapshot(BaseMessage):
    snapshot: Annotated[Snapshot, Field(1)]
    app_hash: Annotated[bytes, Field(2)]


@dataclass
class RequestLoadSnapshotChunk(BaseMessage):
    height: Annotated[int, Field(1)]
    format: Annotated[int, Field(2)]
    chunks: Annotated[int, Field(3)]


@dataclass
class RequestApplySnapshotChunk(BaseMessage):
    index: Annotated[int, Field(1)]
    chunk: Annotated[bytes, Field(2)]
    sender: Annotated[str, Field(3)]


@dataclass
class Request(BaseMessage):
    """ Request types.py """
    value = OneOf[Optional[BaseMessage]]()
    which_one = value.which_one_of_getter()

    echo: Annotated[Optional[RequestEcho], Field(1, one_of=value)] = None
    flush: Annotated[Optional[RequestFlush], Field(2, one_of=value)] = None
    info: Annotated[Optional[RequestInfo], Field(3, one_of=value)] = None
    init_chain: Annotated[Optional[RequestInitChain], Field(5, one_of=value)] = None
    query: Annotated[Optional[RequestQuery], Field(6, one_of=value)] = None
    check_tx: Annotated[Optional[RequestCheckTx], Field(8, one_of=value)] = None
    commit: Annotated[Optional[RequestCommit], Field(11, one_of=value)] = None
    list_snapshots: Annotated[Optional[RequestListSnapshots], Field(12, one_of=value)] = None
    offer_snapshot: Annotated[Optional[RequestOfferSnapshot], Field(13, one_of=value)] = None
    load_snapshot_chunk: Annotated[Optional[RequestLoadSnapshotChunk], Field(14, one_of=value)] = None
    apply_snapshot_chunk: Annotated[Optional[RequestApplySnapshotChunk], Field(15, one_of=value)] = None
    prepare_proposal: Annotated[Optional[RequestPrepareProposal], Field(16, one_of=value)] = None
    process_proposal: Annotated[Optional[RequestProcessProposal], Field(17, one_of=value)] = None
    extend_vote: Annotated[Optional[RequestExtendVote], Field(18, one_of=value)] = None
    verify_vote_extension: Annotated[Optional[RequestVerifyVoteExtension], Field(19, one_of=value)] = None
    finalize_block: Annotated[Optional[RequestFinalizeBlock], Field(20, one_of=value)] = None
