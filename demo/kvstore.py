import csv
import logging
import os.path

from abci.samples import SimpleApp
from abci.types import ResponseCommit, RequestInfo, ResponseInfo, RequestCheckTx, ResponseCheckTx, RequestFinalizeBlock


class KVStore(SimpleApp):
    """ Sample KVStore application """

    def __init__(self):
        super().__init__(lambda value: self._store.__setitem__('__last_block_height', value))
        self._store = dict()
        self._filename = os.path.join(os.path.dirname(__file__), 'kvstore.csv')
        if os.path.exists(self._filename):
            with open(self._filename, 'r') as f:
                csv_reader = csv.reader(f)
                self._store = dict((key, value) for key, value in csv_reader)
        else:
            self._store['__last_block_height'] = 0

    @property
    def last_block_height(self):
        return int(self._store['__last_block_height'])

    async def info(self, req: RequestInfo):
        self.logger.info(f"Connected CometBFT {req.version} via ABCI {req.abci_version}")
        return ResponseInfo("KVStore sample application", "0.1.0", 100, self.last_block_height)

    async def query(self, req):
        resp = await super().query(req)
        if resp.key:
            if value := self._store.get(resp.key.decode('utf8')):
                resp.value = value
                resp.log = "exists"
        return resp

    async def check_tx(self, req: RequestCheckTx):
        try:
            key, value = req.tx.decode('utf8').split('=')
            if key == '__last_block_height':
                raise
        except:
            return ResponseCheckTx(code=1, log="Wrong key=value")

    async def finalize_block(self, req: RequestFinalizeBlock):
        for tx in req.txs:
            key, value = tx.decode('utf8').split('=')
            self._store[key] = value
        return await super().finalize_block(req)

    async def commit(self, _):
        with open(self._filename, 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows([(key, value) for key, value in self._store.items()])
        return ResponseCommit(self.last_block_height)

app = KVStore()
logging.basicConfig(level=logging.DEBUG)
