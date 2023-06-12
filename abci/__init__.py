"""
Asynchronous implementation of the ABCI protocol
"""
from abc import ABC

from .handlers import InfoHandler, MempoolHandler, ConsensusHandler, StateSyncHandler, HasHandlers
from .server import Server
from .protocol import Protocol


class BaseApplication(HasHandlers, InfoHandler, MempoolHandler, ConsensusHandler, ABC):
    """ Abstract base class of a simple monolithic ABCI application
    """

    async def get_connection_handler(self, kind):
        return self
