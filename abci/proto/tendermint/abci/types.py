from dataclasses import dataclass, field

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from typing_extensions import Annotated

from .enums import MisbehaviorType
from ..crypto import PublicKey
from ..types import BlockIDFlag
from ...google.protobuf import Timestamp


@dataclass
class Validator(BaseMessage):
    address: Annotated[bytes, Field(1)] = b""
    power: Annotated[int, Field(3)] = 0


@dataclass
class ValidatorUpdate(BaseMessage):
    pub_key: Annotated[PublicKey, Field(1)]
    power: Annotated[int, Field(2)] = 0


@dataclass
class VoteInfo(BaseMessage):
    """ Extended vote information """
    validator: Annotated[Validator, Field(1)]
    block_id_flag: Annotated[BlockIDFlag, Field(3)] = BlockIDFlag.BLOCK_ID_FLAG_UNKNOWN


@dataclass
class ExtendedVoteInfo(BaseMessage):
    """ Extended vote information

    Attributes:
        validator: The validator that sent the vote.
        vote_extension: Non-deterministic extension provided by the sending validator's application.
        extension_signature: Vote extension signature created by CometBFT
        block_id_flag: indicates whether the validator voted for a block, nil, or did not vote at all
    """
    validator: Annotated[Validator, Field(1)]
    vote_extension: Annotated[bytes, Field(3)] = b""
    extension_signature: Annotated[bytes, Field(4)] = b""
    block_id_flag: Annotated[BlockIDFlag, Field(5)] = BlockIDFlag.BLOCK_ID_FLAG_UNKNOWN


@dataclass
class CommitInfo(BaseMessage):
    """ Commit information """
    round: Annotated[int, Field(1)] = 0
    votes: Annotated[list[VoteInfo], Field(2)] = field(default_factory=list)


@dataclass
class ExtendedCommitInfo(BaseMessage):
    """ ExtendedCommitInfo is similar to CommitInfo except that it is only used in
    the PrepareProposal request such that CometBFT can provide vote extensions
    to the application.

    Attributes:
         round: The round at which the block proposer decided in the previous height.
         votes: List of validators' addresses in the last validator set with their voting
            information, including vote extensions.
    """
    round: Annotated[int, Field(1)] = 0
    votes: Annotated[list[ExtendedVoteInfo], Field(2)] = field(default_factory=list)


@dataclass
class Misbehavior(BaseMessage):
    """ Mis behavior
    Attributes:
        type:
        validator: The offending validator
        height: The height when the offense occurred
        time: The corresponding time where the offense occurred
        total_voting_power: Total voting power of the validator set in case the ABCI application does
            not store historical validators.
    """
    type: Annotated[MisbehaviorType, Field(1)] = None
    validator: Annotated[Validator, Field(2)] = None
    height: Annotated[int, Field(3)] = 0
    time: Annotated[Timestamp, Field(4)] = None
    total_voting_power: Annotated[int, Field(5)] = 0


@dataclass
class EventAttribute(BaseMessage):
    key: Annotated[str, Field(1)] = ""
    value: Annotated[str, Field(2)] = ""
    index: Annotated[bool, Field(3)] = False


@dataclass
class Event(BaseMessage):
    type: Annotated[str, Field(1)] = ""
    attributes: Annotated[list[EventAttribute], Field(2)] = field(default_factory=list)


@dataclass
class ExecTxResult(BaseMessage):
    code: Annotated[int, Field(1)] = 0
    data: Annotated[bytes, Field(2)] = b""
    log: Annotated[str, Field(3)] = ""
    info: Annotated[str, Field(4)] = ""
    gas_wanted: Annotated[int, Field(5)] = 0
    gas_used: Annotated[int, Field(6)] = 0
    events: Annotated[list[Event], Field(7)] = field(default_factory=list)
    codespace: Annotated[str, Field(8)] = ""


@dataclass
class Snapshot(BaseMessage):
    height: Annotated[int, Field(1)]
    format: Annotated[int, Field(2)]
    chunks: Annotated[int, Field(3)]
    hash: Annotated[bytes, Field(4)]
    metadata: Annotated[bytes, Field(5)] = b''
