from dataclasses import dataclass, field
from typing import List

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from typing_extensions import Annotated

from abci.proto.google.protobuf import Duration


@dataclass
class ABCIParams(BaseMessage):
    """ ABCIParams configure functionality specific to the Application Blockchain Interface.

    Attributes:
        vote_extensions_enable_height:
            configures the first height during which vote extensions will be enabled.

            During this specified height, and for all subsequent heights, precommit
            messages that do not contain valid extension data will be considered invalid.
            Prior to this height, vote extensions will not be used or accepted by validators
            on the network.

            Once enabled, vote extensions will be created by the application in ExtendVote,
            passed to the application for validation in VerifyVoteExtension and given
            to the application to use when proposing a block during PrepareProposal.

    """
    vote_extensions_enable_height: Annotated[int, Field(1)] = 0


@dataclass
class HashedParams(BaseMessage):
    """ HashedParams is a subset of ConsensusParams. It is hashed into the Header.ConsensusHash.
    """
    block_max_bytes: Annotated[int, Field(1)] = 0
    block_max_gas: Annotated[int, Field(2)] = 0


@dataclass
class ValidatorParams(BaseMessage):
    """ ValidatorParams restrict the public key types.py validators can use.

    Note: uses ABCI pubkey naming, not Amino names.
    """
    pub_key_types: Annotated[List[str], Field(1)] = field(default_factory=list)


@dataclass
class VersionParams(BaseMessage):
    """ VersionParams contains the ABCI application version. """
    app: Annotated[int, Field(1)] = 0


@dataclass
class EvidenceParams(BaseMessage):
    """ EvidenceParams determine how we handle evidence of malfeasance.

    Attributes:
        max_age_num_blocks:
            Max age of evidence, in blocks.
            The basic formula for calculating this is: MaxAgeDuration / {average block time}.
        max_age_duration:
            Max age of evidence, in time.
            It should correspond with an app's "unbonding period" or other similar
            mechanism for handling [Nothing-At-Stake attacks](https://github.com/ethereum/wiki/wiki/Proof-of-Stake-FAQ#what-is-the-nothing-at-stake-problem-and-how-can-it-be-fixed)
        max_bytes:
            This sets the maximum size of total evidence in bytes that can be committed in a single block.
            and should fall comfortably under the max block bytes. Default is 1048576 or 1MB
    """
    max_age_num_blocks: Annotated[int, Field(1)]
    max_age_duration: Annotated[Duration, Field(2)]
    max_bytes: Annotated[int, Field(3)] = 1048576


@dataclass
class BlockParams(BaseMessage):
    """ BlockParams contains limits on the block size.

    Attributes:
         max_bytes:
            Max block size, in bytes, must be greater than 0
         max_gas:
            Max gas per block, must be greater or equal to -1
    """
    max_bytes: Annotated[int, Field(1)]
    max_gas: Annotated[int, Field(2)]


@dataclass
class ConsensusParams(BaseMessage):
    """ ConsensusParams contains consensus critical parameters
    that determine the validity of blocks.
    """
    block: Annotated[BlockParams, Field(1)] = None
    evidence: Annotated[EvidenceParams, Field(2)] = None
    validator: Annotated[ValidatorParams, Field(3)] = None
    version: Annotated[VersionParams, Field(4)] = None
    abci: Annotated[ABCIParams, Field(5)] = None
