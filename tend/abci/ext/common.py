import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING


@dataclass
class AppState:
    """ Base ABCI application state ""

    Attributes:
        block_height: block height in chain
        app_hash: Hash of the application state
    """
    block_height: int = 0
    app_hash: bytes = b''

    def clone(self) -> 'AppState':
        """ Clones instance """
        return type(self)(**asdict(self))


class HasAppState(ABC):
    """ Has application state interface
    """

    @property
    @abstractmethod
    def state(self) -> 'AppState':
        """ Returns application state
        """


if TYPE_CHECKING:
    from logging import Logger
    from typing import Any

    Options = dict[str, Any]


class HasAppLogger(ABC):
    """ Has application logger
    """

    @property
    @abstractmethod
    def logger(self) -> 'Logger':
        """ Returns application logger
        """


class HasAppOptions(ABC):
    """ Has application options
    """

    @property
    @abstractmethod
    def options(self) -> 'Options':
        """ Returns application options
        """


class CommonApp(HasAppState, HasAppLogger, HasAppOptions, ABC):
    """ Base application class
    """

    def __init__(self, logger: 'Logger' = None):
        self.__logger = logger or logging.root
        self.__options = dict()
        self.__state = None

    @property
    def logger(self) -> 'Logger':
        return self.__logger

    @property
    def options(self) -> 'Options':
        return self.__options

    @property
    def state(self) -> 'AppState':
        return self.__state

    async def update_app_state(self, new_state: 'AppState'):
        if not self.__state:
            self.__state = new_state
        block_height = new_state.block_height
        if block_height > self.state.block_height:
            self.__state = new_state.clone()
        elif block_height == self.state.block_height:
            if new_state.app_hash != self.state.app_hash:
                raise RuntimeError(f"Synchronized block {block_height}, but `app_hash` not matched")


class HasRelatedApp(ABC):
    """ Has related application
    """

    @property
    @abstractmethod
    def app(self) -> 'CommonApp':
        """ Returns related application
        """
