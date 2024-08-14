import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Transport, Task
from collections import deque
from logging import Logger
from typing import Coroutine, Callable

import betterproto
from betterproto import Message

from abci.abc.handlers import HasHandlers, OneOfHandlers, HandlersKind
from abci.abc.handlers import InfoHandler, MempoolHandler, ConsensusHandler, StateSyncHandler
from .pb.tendermint.abci import Request, Response, ResponseEcho, ResponseFlush


class ServerState(ABC):
    """ Server state interface """

    @property
    @abstractmethod
    def connections(self) -> set['Protocol']:
        """ Server connections set """

    @abstractmethod
    async def stop(self):
        """ Stop server """


DoneCallback = Callable[[Task], None]


class MessageTaskProcessor(ABC):
    """ Message task processor interface """

    @abstractmethod
    def __call__(self, coro: 'Coroutine', done_callback: 'DoneCallback'):
        """  Processes request message  """


class RequestOrderedTaskProcessor(MessageTaskProcessor):
    """ Strong request/response ordered message task processor """
    current_task: 'Task' = None
    tasks: deque[('Coroutine', 'DoneCallback')] = deque()

    def __call__(self, coro: 'Coroutine', done_callback: 'DoneCallback'):
        self.tasks.append((coro, done_callback))
        if self.current_task is None:
            self._turn()

    def _done(self, task, done_callback):
        self.current_task = None
        done_callback(task)

    def _turn(self):
        if self.current_task is None and len(self.tasks):
            coro, done_callback = self.tasks.popleft()
            self.current_task = task = asyncio.create_task(coro)
            task.add_done_callback(lambda task: self._done(task, done_callback))
            task.add_done_callback(lambda _: self._turn())


class ResponseOrderedTaskProcessor(MessageTaskProcessor):
    """ Only response ordered message task processor """
    tasks: deque[('Task', 'DoneCallback')] = deque()

    def __call__(self, coro: 'Coroutine', done_callback: 'DoneCallback'):
        task = asyncio.create_task(coro)
        self.tasks.append((task, done_callback))
        task.add_done_callback(lambda _: self._turn())

    def _turn(self):
        while len(self.tasks):
            task, done_callback = self.tasks[0]
            if task.done():
                try:
                    done_callback(task)
                finally:
                    self.tasks.popleft()
            else:
                break


class Protocol(asyncio.Protocol):
    """ ABCI Protocol
    """
    buffer: bytes = b''
    transport: 'Transport'
    handler: 'OneOfHandlers' = None
    handler_type: 'HandlersKind' = None

    def __init__(self, app: 'HasHandlers', server_state: 'ServerState', logger: 'Logger' = None):
        self.message_processor = ResponseOrderedTaskProcessor()
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
        self.server_state.connections.discard(self)
        if self.handler:
            self.logger.info(f"Connection from {':'.join(map(str, self.remote))} closed")
        if len(self.server_state.connections) == 0:
            asyncio.create_task(self.server_state.stop())

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
        if self.handler_type is None:
            if name in ('info', 'set_option', 'query'):
                self.handler_type = InfoHandler
                self.logger.info(f"InfoConnection from {':'.join(map(str, self.remote))} established")
            elif name in ('init_chain', 'begin_block', 'deliver_tx', 'end_block', 'commit'):
                self.handler_type = ConsensusHandler
                self.message_processor = RequestOrderedTaskProcessor()
                self.logger.info(f"ConsensusConnection from {':'.join(map(str, self.remote))} established")
            elif name in ('list_snapshots', 'offer_snapshot', 'load_snapshot_chunk', 'apply_snapshot_chunk'):
                self.handler_type = StateSyncHandler
                self.logger.info(f"StateSyncConnection from {':'.join(map(str, self.remote))} established")
            elif name == 'check_tx':
                self.handler_type = MempoolHandler
                self.logger.info(f"MempoolConnection from {':'.join(map(str, self.remote))} established")

        async def async_response(name: str, message: 'Message') -> 'Message':
            if self.handler is None and self.handler_type is not None:
                self.handler = await self.app.get_connection_handler(self.handler_type)
            try:
                if self.handler is not None:
                    if handler := getattr(self.handler, name, None):
                        return await handler(message)
                if name == 'echo':
                    return ResponseEcho(message.message)
                elif name == 'flush':
                    return ResponseFlush()
                raise NotImplementedError(f'Async ABCI Method `{name}` is not implemented')
            except:
                logging.critical(f'Async ABCI method `{name}` has failed')
                raise

        self.message_processor(async_response(name, message), lambda task: self.process_task_done(name, task))

    def process_task_done(self, name: str, task: 'Task'):
        try:
            if task.exception():
                raise task.exception()
            else:
                self.process_response(name, task.result())
        except Exception as exc:
            logging.exception(exc, exc_info=True)
            self.transport.abort()

    def process_response(self, name: str, message: 'Message'):
        data = bytes(Response(**{name: message}))
        self.transport.write(betterproto.encode_varint(len(data) << 1))
        self.transport.write(data)
