from enum import IntEnum


class MisbehaviorType(IntEnum):
    UNKNOWN = 0
    DUPLICATE_VOTE = 1
    LIGHT_CLIENT_ATTACK = 2


class ProposalStatus(IntEnum):
    UNKNOWN = 0
    ACCEPT = 1
    REJECT = 2


class CheckTxType(IntEnum):
    NEW = 0
    RECHECK = 1


class VerifyStatus(IntEnum):
    UNKNOWN = 0
    ACCEPT = 1
    REJECT = 2


class OfferSnapshotResult(IntEnum):
    UNKNOWN = 0
    ACCEPT = 1
    ABORT = 2
    REJECT = 3
    REJECT_FORMAT = 4
    REJECT_SENDER = 5


class ApplySnapshotChunkResult(IntEnum):
    UNKNOWN = 0
    ACCEPT = 1
    ABORT = 2
    RETRY = 3
    RETRY_SNAPSHOT = 4
    REJECT_SNAPSHOT = 5
