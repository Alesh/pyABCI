from abci.proto.tendermint import (
    Request, RequestInfo, RequestFlush, RequestEcho, Response, ResponseEcho, ResponseFlush,
    ResponseInfo
)


def test_request_message():
    req = Request(echo=RequestEcho(message="Hello!"))
    req = Request.loads(req.dumps())
    assert isinstance(req.value, RequestEcho)

    req = Request(flush=RequestFlush())
    req = Request.loads(req.dumps())
    assert isinstance(req.value, RequestFlush)

    req = Request(info=RequestInfo(version='0.38.11', block_version=11, p2p_version=8, abci_version='2.0.0'))
    req = Request.loads(req.dumps())
    assert isinstance(req.value, RequestInfo) and req.value.version == '0.38.11'


def test_response_message():
    resp = Response(echo=ResponseEcho(message="Hello!"))
    resp = Response.loads(resp.dumps())
    assert isinstance(resp.value, ResponseEcho)

    resp = Response(flush=ResponseFlush())
    resp = Response.loads(resp.dumps())
    assert isinstance(resp.value, ResponseFlush)

    resp = Response(info=ResponseInfo(version='0.38.11', app_version=7, last_block_height=0))
    resp = Response.loads(resp.dumps())
    assert isinstance(resp.value, ResponseInfo) and resp.info.version == '0.38.11'
