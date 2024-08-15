import asyncio
import signal
import sys

from abci.abc import Application
from abci.abc.connections import ConnectionsHolder
from abci.protocol import Protocol


class Server(ConnectionsHolder):
    """ ABCI 2.0 application server
    """

    def __init__(self, app: Application):
        super().__init__(logger=app.logger, on_empty=self.stop)
        self.app = app
        self._server: asyncio.Server | None = None
        self._run_forever: asyncio.Task | None = None

    def __await__(self):
        return self._run_forever.__await__()

    @property
    def active(self):
        """ True if the server is active """
        return self._server is not None and self._server.is_serving()

    async def start(self, host='127.0.0.1', port=26658, **server_opts):
        """ Start the server """
        loop = asyncio.get_running_loop()
        if sys.platform.lower() != "windows":
            loop.add_signal_handler(signal.SIGINT, lambda: self.stop())
            loop.add_signal_handler(signal.SIGTERM, lambda: self.stop())
        self._server = await loop.create_server(lambda: Protocol(self.app, self), host, port, **server_opts)
        self.logger.info(f"ABCI server is listening on {host}:{port}")

        async def run_forever():
            try:
                async with self._server:
                    await self._server.serve_forever()
            except (KeyboardInterrupt, asyncio.CancelledError):
                self.stop()
                if self._server.is_serving():
                    loop.run_until_complete(self._server.wait_closed())
            finally:
                self.logger.info("ABCI server has stopped")

        self._run_forever = asyncio.create_task(run_forever())

    def stop(self):
        """ Stop the server """
        for connection in self.connections:
            connection.transport.close()
        self._server.close()


def main():
    import getopt
    from abci.utils import resolve_app

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ap", ["host=", "port="])
        if len(args) != 1:
            raise getopt.GetoptError("Wrong number of arguments")
    except getopt.GetoptError as err:
        print(err)
        print(f"Usage: {sys.argv[0]} -h | --help")
        sys.exit(2)

    app = resolve_app(args[0])
    host = '127.0.0.1'
    port = 26658
    for opt, value in opts:
        if opt in ("-a", "--host"):
            host = value
        elif opt in ("-p", "--port"):
            port = int(value)
        else:
            assert False, "unhandled option"

    async def async_run():
        server = Server(app)
        await server.start(host, port)
        await server

    asyncio.run(async_run())


if __name__ == '__main__':
    main()
