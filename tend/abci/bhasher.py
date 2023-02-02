import hashlib
from abc import ABC, abstractmethod


class Hasher(ABC):
    """ Base hasher class
    """

    @property
    @abstractmethod
    def size(self) -> int:
        """ Hash size in bytes
        """

    @property
    @abstractmethod
    def block_size(self) -> int:
        """ Max data block size
        """

    @abstractmethod
    def write(self, block: bytes) -> int:
        """ Sends data block to hash.
        """

    @abstractmethod
    def sum(self, prefix: bytes = None) -> bytes:
        """ Returns current hash with prefix if present. It does not reset hasher.
        """


class BlockHasher(ABC):
    """ Block hasher interface
    """

    @property
    @abstractmethod
    def size(self) -> int:
        """ Hash size in bytes
        """

    @property
    @abstractmethod
    def block_size(self) -> int:
        """ Hasher Internal block size """

    @abstractmethod
    def write_tx(self, block: bytes) -> bytes:
        """ Sends tx data to hash to build block hash.

        Returns:
            Digest (Hash of TX)
        """

    @abstractmethod
    def write_hash(self, block: bytes):
        """ Sends ready tx data hash to build block hash.
        """

    @abstractmethod
    def sum(self, prefix: bytes = None) -> bytes:
        """ Returns current block hash with prefix if present.  It does not reset hasher.
        """


class DummyBlockHasher(BlockHasher):
    """ Dummy implementation of block hasher
    """

    class Sha256Wrapper(Hasher):
        def __init__(self):
            self._raw = hashlib.sha256()

        @property
        def size(self) -> int:
            return self._raw.digest_size

        @property
        def block_size(self) -> int:
            return self._raw.block_size

        def write(self, block: bytes) -> int:
            self._raw.update(block[:self.block_size])
            return len(block[:self.block_size])

        def sum(self, prefix: bytes = None) -> bytes:
            return (prefix or b'') + self._raw.digest()

    def __init__(self, hasher_factory=None):
        self._tx_hashes = list()
        self._hasher_factory = hasher_factory or (lambda: self.Sha256Wrapper())
        self._hasher = self._hasher_factory()

    @property
    def size(self) -> int:
        return self._hasher.size

    @property
    def block_size(self) -> int:
        return self._hasher.block_size

    def write_tx(self, tx_data: bytes) -> bytes:
        hasher = self._hasher_factory()
        bs = hasher.block_size
        for chunk in [tx_data[n * bs:(n + 1) * bs] for n in range(len(tx_data) // bs + 1)]:
            hasher.write(chunk)
        tx_hash = hasher.sum()
        self.write_hash(tx_hash)
        return tx_hash

    def write_hash(self, tx_hash: bytes):
        if tx_hash in self._tx_hashes:
            raise ValueError('Received TX hash duplicate')
        self._tx_hashes.append(tx_hash)

    def sum(self, prefix: bytes = None) -> bytes:
        hasher = self._hasher_factory()
        for tx_hash in self._tx_hashes:
            hasher.write(tx_hash)
        return hasher.sum(prefix)
