import asyncio
import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import TYPE_CHECKING

import betterproto

from .handlers import InfoHandler, ConsensusHandler, StateSyncHandler, MempoolHandler
from ..pb.tendermint.abci import Request, Response, ResponseEcho, ResponseFlush

if TYPE_CHECKING:
    from asyncio import Transport, Task
    from typing import Coroutine
    from betterproto import Message
    from logging import Logger
    from .handlers import OneOfHandlers, HasHandlers


class ServerState(ABC):
    """ Server state """

    @property
    @abstractmethod
    def connections(self) -> set['Protocol']:
        """ Server connections set """


class Protocol(asyncio.Protocol):
    """ ABCI Protocol
    """
    buffer: bytes = b''
    transport: 'Transport'
    handler: 'OneOfHandlers' = None
    current_task: 'Task' = None
    timeout: int | float = 300

    def __init__(self, app: 'HasHandlers', server_state: 'ServerState', logger: 'Logger' = None):
        self.tasks: deque[tuple[str, 'Coroutine']] = deque()
        self.logger = logger or logging.root
        self.server_state = server_state
        self.app = app

    @property
    def remote(self):
        if self.transport:
            return self.transport.get_extra_info('peername')[:2]

    def connection_made(self, transport: 'Transport'):
        self.transport = transport
        self.server_state.connections.add(self)

    def connection_lost(self, exc: Exception | None) -> None:
        assert len(self.tasks) > 0
        self.server_state.connections.discard(self)

    def data_received(self, data: bytes):
        self.buffer += data
        while len(self.buffer):
            result, pos = betterproto.decode_varint(self.buffer, 0)
            length = result >> 1
            if len(self.buffer) >= pos + length:
                data, self.buffer = self.buffer[pos:pos + length], self.buffer[pos + length:]
                name, message = betterproto.which_one_of(Request().parse(data), "value")
                self.process_request(name, message)
            else:
                break

    def process_request(self, name: str, message: 'Message'):

        async def async_response(name: str, message: 'Message') -> 'Message':
            # create handler if missing
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

            # process message
            async def response_echo():
                return ResponseEcho(message.message)

            async def response_flush():
                return ResponseFlush()

            try:
                response = None
                if self.handler is not None:
                    if handler := getattr(self.handler, name, None):
                        response = await handler(message)
                if response is None:
                    if name == 'echo':
                        response = await response_echo()
                    elif name == 'flush':
                        response = await response_flush()
                if response is None:
                    raise NotImplementedError(f'Async ABCI Method `{name}` is not implemented')
                return response
            except:
                logging.exception(f'Async ABCI method `{name}` has failed', exc_info=True)
                raise

        self.tasks.append((name, async_response(name, message)))
        self.process_response_tasks()

    def process_response_tasks(self):
        def response_task_done(task):
            try:
                if task.exception():
                    raise task.exception()
                else:
                    self.process_response(name, task.result())
            finally:
                self.tasks.popleft()
                self.current_task = None
                self.process_response_tasks()

        # ToDo: determined (ordered) execution only for ConsensusHandler, other may be executed cooperative
        if self.current_task is None and len(self.tasks):
            name, coro = self.tasks[0]
            self.current_task = asyncio.create_task(coro)
            self.current_task.add_done_callback(response_task_done)

    def process_response(self, name: str, message: 'Message'):
        data = bytes(Response(**{name: message}))
        self.transport.write(betterproto.encode_varint(len(data) << 1))
        self.transport.write(data)
