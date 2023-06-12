# Samples of using **AsyncABCI**  

## Counter application
First you need to run docker-compose with the tendermint node and then need 
to install **AsyncABC** with the following commands, preferably in a virtual 
environment. 
```shell
pip install git+https://github.com/Alesh/AsyncABCI.git@master
```
After that, run the "pure" or "extend" version of the application.
 * `counter_simple.py` -- ABCI application built with **pure API**
 * `counter_extend.py` -- ABCI application built with **extended API**

ex. "extend"
```shell
python3 counter_extend.py
```
From another console, you can interact with the running application and the 
tendermint with the following commands:
```shell
curl http://localhost:26657/broadcast_tx_commit?tx=0x00100500
curl http://localhost:26657/broadcast_tx_commit?tx=0x00100501
curl -s 'http://localhost:26657/abci_query?path="height"' | jq .result.response.value --raw-output | base64 --decode
curl -s 'http://localhost:26657/abci_query?path="counter"' | jq .result.response.value --raw-output | base64 --decode
curl http://localhost:26657/broadcast_tx_commit?tx=0x00100502
curl -s 'http://localhost:26657/abci_query?path="counter"' | jq .result.response.value --raw-output | base64 --decode
curl -s 'http://localhost:26657/abci_query?path="height"' | jq .result.response.value --raw-output | base64 --decode
curl -s 'http://localhost:26657/abci_query?path="hash"' | jq .result.response.value --raw-output
```