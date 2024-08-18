## `KVStore` sample ABCI application

To run the KVStore example, do the following:

* If you are running it for the first time, you need to initialize the home directory of
  the single CometBFT node: `cometbft init --home ./.cometbft`
* Run a single CometBFT node with the following parameters: `cometbft start --home ./.cometbft`
* From this directory, launch the application: `python -m abci.server kvstore:app`
* The `pyABCI` package must be installed.

Check it:

  ```shell
  curl -s 'localhost:26657/broadcast_tx_commit?tx="name=satoshi"'
  curl -s 'localhost:26657/abci_query?data="name"'
  ```