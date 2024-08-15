## `KVStore` sample ABCI application

To run the KVStore example, do the following:
* Run a single CometBFT node with the following parameters: `cometbft start --home ./.cometbft`
* From this directory, launch the application: `python -m abci.server kvstore:app` 
* The `pyABCI` package must be installed.
