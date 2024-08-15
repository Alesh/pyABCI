import asyncio
import logging
from io import BytesIO

from pure_protobuf.io.varint import read_unsigned_varint, write_unsigned_varint

from abci.abc import Application
from abci.abc.connections import ConnectionProtocol, ConnectionsHolder
from abci.types import Request, Response, ResponseEcho, ResponseFlush


class Protocol(ConnectionProtocol):
    """ ABCI 2.0 protocol implementation.
    """

    def __init__(self, app: Application, holder: ConnectionsHolder):
        super().__init__(holder)
        self.requests_task = None  # type: asyncio.Task | None
        self.requests_queue = asyncio.Queue()
        self.app = app
        self.buffer = b''

    def connection_made(self, transport: asyncio.Transport):
        super().connection_made(transport)
        self.requests_task = asyncio.create_task(self.request_processor())

    def connection_lost(self, exc: Exception | None):
        if self.requests_task:
            self.requests_task.cancel()
        self.requests_task = None
        super().connection_lost(exc)

    def data_received(self, data: bytes):
        self.buffer += data
        while len(self.buffer):
            stream = BytesIO(self.buffer)
            size = read_unsigned_varint(stream)
            pos = stream.tell()
            if len(self.buffer) < pos + size:
                break
            stream, self.buffer = BytesIO(self.buffer[pos:][:size]), self.buffer[pos + size:]
            self.requests_queue.put_nowait(Request.read_from(stream))

    async def request_processor(self):
        try:
            while self.transport.is_reading():
                request = await self.requests_queue.get()
                try:
                    if request.which_one() == 'flush':
                        response = Response(flush=ResponseFlush())
                    elif request.which_one() == 'echo':
                        response = ResponseEcho(message=request.echo.message)
                    else:
                        response = await self.app(request)
                    data = response.dumps()
                    write_unsigned_varint(len(data), self.transport)
                    self.transport.write(data)
                except Exception as exc:
                    raise RuntimeError(f"Error occurred while processing {request.which_one()} request") from exc
            else:
                return
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception(exc)
            else:
                self.logger.error(exc)
        finally:
            self.transport.close()
