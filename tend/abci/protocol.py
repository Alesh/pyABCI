import asyncio
import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import TYPE_CHECKING

import betterproto

from .handlers import InfoHandler, MempoolHandler, ConsensusHandler, StateSyncHandler
from ..pb.tendermint.abci import Request, Response, ResponseFlush, ResponseEcho

if TYPE_CHECKING:
    from asyncio import Transport, Task
    from betterproto import Message
    from logging import Logger
    from .handlers import OneOfHandlers, HasHandlers


class ServerState(ABC):
    """ Server state """

    @property
    @abstractmethod
    def connections(self) -> set['Protocol']:
        """ Server connections set """

    @property
    @abstractmethod
    def tasks(self) -> set['Task']:
        """ Connections task set """


class Protocol(asyncio.Protocol):
    """ ABCI Protocol
    """
    buffer: bytes = b''
    transport: 'Transport'
    handler: 'OneOfHandlers' = None

    def __init__(self, app: 'HasHandlers', server_state: 'ServerState', logger: 'Logger' = None):
        self.logger = logger or logging.root
        self.server_state = server_state
        self.response_queue = deque()
        self.app = app

    @property
    def remote(self):
        if self.transport:
            return self.transport.get_extra_info('peername')[:2]

    def connection_made(self, transport: 'Transport'):
        self.transport = transport
        self.server_state.connections.add(self)

    def connection_lost(self, exc: Exception | None) -> None:
        self.server_state.connections.discard(self)

    def data_received(self, data: bytes):
        self.buffer += data
        while len(self.buffer):
            result, pos = betterproto.decode_varint(self.buffer, 0)
            length = result >> 1
            if len(self.buffer) >= pos + length:
                data, self.buffer = self.buffer[pos:pos + length], self.buffer[pos + length:]
                name, message = betterproto.which_one_of(Request().parse(data), "value")
                match name:
                    case 'flush':
                        if len(self.response_queue):
                            self.response_queue.append((name, ResponseFlush()))
                        else:
                            self.process_response(name, ResponseFlush())
                    case 'echo':
                        self.process_response(name, ResponseEcho(message.message))
                    case _:
                        self.process_request(name, message)
            else:
                break

    def process_request(self, name: str, message: 'Message'):

        async def async_response() -> 'Message':
            if self.handler is None:
                if name in ('info', 'set_option', 'query'):
                    self.handler = await self.app.get_connection_handler(InfoHandler)
                    self.logger.info(f"InfoConnection from {':'.join(map(str, self.remote))} established")
                elif name in ('init_chain', 'begin_block', 'deliver_tx', 'end_block', 'commit'):
                    self.handler = await self.app.get_connection_handler(ConsensusHandler)
                    self.logger.info(f"ConsensusConnection from {':'.join(map(str, self.remote))} established")
                elif name in ('list_snapshots', 'offer_snapshot', 'load_snapshot_chunk', 'apply_snapshot_chunk'):
                    self.logger.info(f"StateSyncConnection from {':'.join(map(str, self.remote))} established")
                    self.handler = await self.app.get_connection_handler(StateSyncHandler)
                elif name == 'check_tx':
                    self.handler = await self.app.get_connection_handler(MempoolHandler)
                    self.logger.info(f"MempoolConnection from {':'.join(map(str, self.remote))} established")
                else:
                    raise NotImplementedError(f'Received not implemented message `{name}`')
            if handler := getattr(self.handler, name, None):
                # ToDo: Rechecks latter. Message `end_block` must be processed after a last `deliver_tx`
                while (name == 'end_block' and
                       len([task for task in self.response_queue if isinstance(task, asyncio.Task)]) > 1):
                    await asyncio.sleep(0.001)
                return await handler(message)
            else:
                raise NotImplementedError(f'Async ABCI Method `{name}` is not implemented for {handler}')

        task = asyncio.create_task(async_response())
        self.server_state.tasks.add(task)
        self.response_queue.append(task)
        task.add_done_callback(lambda task: self.process_response_task(task, name))

    def process_response_task(self, task: 'Task', name: str):
        try:
            self.process_response(name, task.result())
        except NotImplementedError as exc:
            args = exc.args or (f'Async ABCI Method `{name}` is not implemented',)
            raise NotImplementedError(*args)
        finally:
            self.server_state.tasks.discard(task)
            self.response_queue.remove(task)
            while (self.response_queue and
                   isinstance(self.response_queue[0], tuple) and
                   self.response_queue[0][0] == 'flush'):
                self.process_response(*self.response_queue.popleft())

    def process_response(self, name: str, message: 'Message'):
        data = bytes(Response(**{name: message}))
        self.transport.write(betterproto.encode_varint(len(data) << 1))
        self.transport.write(data)
