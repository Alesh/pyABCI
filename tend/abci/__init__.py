"""
Asyncore implementation of ABCI protocol
"""
from enum import IntEnum

from .handlers import BaseApplication, InfoHandler, MempoolHandler, ConsensusHandler, StateSyncHandler
from .server import Server, ServerState
from .protocol import Protocol


class ResultCode(IntEnum):
    """ ABCI result codes enum
    """
    OK = 0
    Error = 1
