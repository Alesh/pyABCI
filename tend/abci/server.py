import asyncio
import logging
import signal
import sys
from asyncio import CancelledError
from typing import TYPE_CHECKING

from .protocol import Protocol, ServerState

if TYPE_CHECKING:
    from asyncio import Task
    from logging import Logger
    from .handlers import HasHandlers


class Server(ServerState):
    """ ABCI application server
    """
    connections: set['Protocol'] = set()
    tasks: set['Task'] = set()

    def __init__(self, app: 'HasHandlers', logger: 'Logger' = None):
        self._srv = None
        self.logger = logger or logging.root
        self.app = app

    async def start(self, host='0.0.0.0', port=26658, **server_options):
        if self._srv is not None:
            raise RuntimeError('Already started')

        def make_connection():
            conn = Protocol(self.app, self, self.logger)
            return conn

        loop = asyncio.get_running_loop()
        on_windows = sys.platform.lower() == "windows"
        if not on_windows:
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.stop()))
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self.stop()))

        self._srv = await loop.create_server(make_connection, host, port, **server_options)
        try:
            async with self._srv:
                self.logger.info(f"ABCI server is listening on {host}:{port}")
                await self._srv.serve_forever()
        except CancelledError:
            pass
        except KeyboardInterrupt:
            loop.run_until_complete(self.stop())
        finally:
            self.logger.info("ABCI server has stopped")
            self._srv = None

    async def stop(self):
        if self._srv is not None:
            self.logger.info("ABCI server is stopping ... ")
            tasks = [*self.tasks]
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
            self._srv.close()
            await self._srv.wait_closed()
