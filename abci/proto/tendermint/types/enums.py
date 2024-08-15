from enum import IntEnum


class BlockIDFlag(IntEnum):
    BLOCK_ID_FLAG_UNKNOWN = 0
    BLOCK_ID_FLAG_ABSENT = 1
    BLOCK_ID_FLAG_COMMIT = 2
    BLOCK_ID_FLAG_NIL = 3
