import asyncio
import logging
import subprocess
import os.path
import tempfile
from base64 import b64decode

import httpx
import pytest
import pytest_asyncio

import abci.server
import abci.utils


@pytest.fixture()
def cometbft():
    args = ['docker', 'run', '--net=host', '-d', 'cometbft/cometbft:v0.38.x',
            'start', '--proxy_app', 'tcp://127.0.0.1:26658']
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        logging.error(proc.stderr.decode('utf8'))
        assert proc.returncode == 0
    container_id = proc.stdout.decode('utf8').strip()
    yield container_id[:12]
    subprocess.run(['docker', 'rm', '-f', container_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


@pytest_asyncio.fixture()
async def kvstore():
    curr_path = os.path.abspath(os.path.curdir)
    filename = os.path.join(os.path.dirname(__file__), '..', 'demo', 'kvstore.py')
    app = abci.utils.resolve_app(f'{filename}:app')
    assert app
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        srv = abci.server.Server(app)
        await srv.start()
        count_down = 10
        while count_down:
            await asyncio.sleep(1)
            if len(srv.connections):
                break
            count_down -= 1
        yield srv
        srv.stop()
        await srv
    os.chdir(curr_path)

@pytest.mark.asyncio
async def test_kvstore(cometbft, kvstore):
    assert cometbft
    assert kvstore
    async with httpx.AsyncClient(base_url='http://localhost:26657') as client:
        resp = await client.get('/status')
        assert resp.status_code == 200
        result = resp.json()
        assert result['result']['node_info']['protocol_version']['app'] == '100'

        resp = await client.get('/broadcast_tx_commit?tx="name=satoshi"')
        assert resp.status_code == 200
        result = resp.json()
        assert result['result']['tx_result']['code'] == 0

        resp = await client.get('/abci_query?data="name"')
        assert resp.status_code == 200
        result = resp.json()
        assert result['result']['response']['code'] == 0
        assert b64decode(result['result']['response']['value']).decode() == 'satoshi'


        resp = await client.get('/broadcast_tx_commit?tx="my=Alesh"')
        assert resp.status_code == 200
        result = resp.json()
        assert result['result']['tx_result']['code'] == 0

        resp = await client.get('/abci_query?data="my"')
        assert resp.status_code == 200
        result = resp.json()
        assert result['result']['response']['code'] == 0
        assert b64decode(result['result']['response']['value']).decode() == 'Alesh'

