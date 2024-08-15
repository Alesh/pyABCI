import logging
import typing as t
from asyncio import BaseTransport, Transport, Protocol


class ConnectionsHolder:
    """ Connection holder
    """

    def __init__(self, logger: logging.Logger = None, on_empty: t.Callable[[], None] = None):
        self.__connections: dict['ConnectionProtocol', Transport] = dict()
        self.__logger = logger or logging.root
        self.__on_empty = on_empty

    @property
    def logger(self) -> logging.Logger:
        """ Logger """
        return self.__logger

    @property
    def connections(self) -> tuple['ConnectionProtocol', ...]:
        return tuple(self.__connections.keys())

    def connection_made(self, protocol: 'ConnectionProtocol', transport: BaseTransport):
        """ Client connection made """
        self.__connections[protocol] = t.cast(Transport, transport)

    def connection_lost(self, protocol: 'ConnectionProtocol', exc: Exception = None):
        """ Client connection lost """
        transport = self.__connections.pop(protocol)
        msg = f"Connection from {':'.join(map(str, transport.get_extra_info('peername')[:2]))} closed"
        if exc:
            msg += f" by error: {exc}"
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception(msg)
            else:
                self.logger.error(msg)
        else:
            self.logger.debug(msg)
        if len(self.__connections) == 0 and self.__on_empty:
            self.__on_empty()


class ConnectionProtocol(Protocol):
    """ Client connection protocol
    """

    def __init__(self, holder: ConnectionsHolder):
        self.__transport: Transport | None = None
        self._holder = holder

    @property
    def transport(self) -> Transport:
        """ Connection transport """
        return self.__transport

    @property
    def logger(self) -> logging.Logger:
        """ Logger """
        return self._holder.logger

    def connection_made(self, transport):
        """ Client connection made """
        self.__transport = transport
        self._holder.connection_made(self, transport)

    def connection_lost(self, exc):
        """ Client connection lost """
        self._holder.connection_lost(self, exc)
