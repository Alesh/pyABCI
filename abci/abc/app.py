import logging
from typing import Union
from abc import ABC, abstractmethod

from abci.abc.handlers import ConsensusHandler, InfoHandler, MempoolHandler, StateSyncHandler
from abci.proto.tendermint.abci import Request, Response


class Application(ABC):
    """ ABCI 2.0 application interface
    """

    @property
    @abstractmethod
    def logger(self) -> logging.Logger:
        """ Application logger. """

    @property
    @abstractmethod
    def handlers(self) -> Union[ConsensusHandler, InfoHandler, MempoolHandler, StateSyncHandler] :
        """ ABCI application handlers """

    async def __call__(self, request: Request) -> Response:
        name = request.which_one()
        if method := getattr(self.handlers, name):
            resp = await method(getattr(request, name))
            return Response(**{name: resp})
        else:
            raise NotImplementedError(f"Not yet implemented request handler {name or ''}")
