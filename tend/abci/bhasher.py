import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable

    OnFinish = Callable[[bytes], None]


class TxHasher:
    """ TX hasher
    """

    def __init__(self, on_finish: 'OnFinish'):
        self._on_finish = on_finish
        self._hasher = hashlib.sha256()

    def __call__(self, data: bytes):
        self.write(data)
        return self.sum()

    def write(self, data: bytes):
        return self._hasher.update(data)

    def sum(self):
        result = self._hasher.digest()
        self._on_finish(result)
        return result


class BlockHasher:
    """ Block hasher
    Interface and simple implementation of chain block hasher.
    """

    def __init__(self):
        self._tx_hashes = list()

    @property
    def tx_hasher(self) -> TxHasher:
        """ Returns TX hasher """
        assert self._tx_hashes.append not in self._tx_hashes
        return TxHasher(self._tx_hashes.append)

    def sum(self) -> bytes:
        """ Returns root block hash """
        hasher = hashlib.sha256()
        for item in self._tx_hashes:
            hasher.update(item)
        return hasher.digest()
