import numpy as np

from .varint import encode_varint

from abc import ABC, abstractmethod
from typing import Tuple, Optional, Iterable, Any
from io import RawIOBase
from struct import pack
from itertools import chain

BLOCKSIZE = 16

class Component(ABC):
    """Abstract base class for Ziggurat data components."""

    def __init__(self, component_type: int, mode: int, name: str, params: Tuple[Optional[int], Optional[int]]) -> None:
        """
        Instantiates a new Component object with all necessary data.

        Parameters
        ----------
        component_type : int
        mode : int
        name : str
            Name of the compenent, maximum lenght = 12.
        params : Tuple[Optional[int], Optional[int]]
            Two arbitrary parameters set by the spec or the user.

        Returns
        -------
        A new (immutable) Component object.
        """

        assert len(name.encode('ascii')) <= 12
        self.component_type = component_type
        self.mode = mode
        self.name = name
        self.params = params


    def write_bom(self, f: RawIOBase, offset: int, size: int) -> None:
        """
        Writes the BOM entry for the component to f.

        Parameters
        ----------
        f : RawIOBase
            A raw binary IO stream(-like object).
        offset: int
            The offset of the component within the container file.
        size: int
            The size in bytes of the component.
        """

        name = self.name.encode('ascii')
        assert len(name) <= 12

        f.write(pack('B', 1))
        f.write(pack('B', self.component_type))
        f.write(pack('B', self.mode))
        f.write(name.ljust(13, b'\0'))
        f.write(pack('<q', offset))
        f.write(pack('<q', size))
        f.write(pack('<q', self.params[0] if self.params[0] else 0))
        f.write(pack('<q', self.params[1] if self.params[1] else 0))


    @abstractmethod
    def bytelen(self) -> int:
        """Returns the length of the componen int bytes."""
        pass


    @abstractmethod
    def write(self, f: RawIOBase) -> None:
        """
        Writes the complete component to f.

        Parameters
        ----------
        f : RawIOBase
            A raw binary IO stream(-like object).
        """
        pass


class Vector(Component):
    
    def __init__(self, items: Iterable[Any], name:str, n: int, d: int = 1,):
        super().__init__(
            0x04,
            0x00,
            name,
            (n, d)
        )
        self.n = n
        self.d = d
        self.data = np.atleast_2d(np.array(items, dtype=np.int64))
        self.data.shape = (d, n)

    
    def bytelen(self):
        return self.n * self.d * 8 

    
    def write(self, f):

        for i in range(self.n):
            for j in range(self.d):
                f.write(pack('<q', self.data[j][i]))


class VectorDelta(Component):

    def __init__(self, items: Iterable[Any], name:str, n: int, d: int = 1,):
        super().__init__(
            0x04,
            0x02,
            name,
            (n, d)
        )
        self.n = n
        self.d = d
        data = np.atleast_2d(np.array(items, dtype=np.int64))
        data.shape = (n, d)

        self.data = data # TODO entfernen
        # compress data

        m = int((n - 1) / BLOCKSIZE) + 1
        delta_start = m*8

        # VarInt encoded blocks
        blocks = []

        for i in range(0, n, BLOCKSIZE):
            block = data[i : i+BLOCKSIZE]
            delta = np.empty(block.shape, dtype=np.int64)
            delta[0] = np.copy(block[0])

            cols = []

            for j in range(d):
                for i in range(1, len(block)):
                    delta[i][j] = block[i][j] - block[i-1][j]

                row = delta[:, j]
                varints = []
                for i in row:
                    varints.append(encode_varint(i))

                cols.append(b''.join(varints))

            blocks.append(b''.join(cols))

        assert len(blocks) == m

        # Sync offsets
        sync = [delta_start]
        for i, b in enumerate(blocks[:-1], start=1):
            sync.append(sync[i-1] + len(b))

        assert len(sync) == m

        self.encoded = b''.join(pack('<q', s) for s in sync) +\
            b''.join(blocks)

    
    def bytelen(self):
        return len(self.encoded)

    
    def write(self, f):
        f.write(self.encoded)


class StringList(Component):

    def __init__(self, strings: Iterable[bytes], name: str, n: int):
        """strings: series of utf-8 encoded null terminated strings"""

        self.encoded = b''
        n = 0

        for _, s in enumerate(strings):
            self.encoded += (s + b'\0')
            n += 1
        
        super().__init__(
            0x02,
            0x00,
            name,
            (n, 0)
        )


    def bytelen(self):
        return len(self.encoded)


    def write(self, f):
        f.write(self.encoded)


class Index(Component):

    def __init__(self, pairs: Iterable[Tuple[int, int]], name: str, n: int, sorted=False):

        super().__init__(
            0x06,
            0x00,
            name,
            (n, 2)
        )
        
        self.data = np.array(pairs, dtype=np.uint64)
        self.data.shape = (n, 2)

        if not sorted:
            self.data = self.data[self.data[:,1].argsort()]
            self.data = self.data[self.data[:,0].argsort(kind='mergesort')]


    def bytelen(self):
        return len(self.data) * 2 * 8


    def write(self, f):
        for i in self.data.flat:
            f.write(pack('<Q', i))


class IndexCompressed(Component):

    def __init__(self, pairs: Iterable[Tuple[int, int]], name: str, n: int, sorted=False):
        
        super().__init__(
            0x06,
            0x01,
            name,
            (n, 2)
        )

        data = np.array(pairs, dtype=np.uint64)
        data.shape = (n, 2)

        if not sorted:
            data = data[data[:,1].argsort()]
            data = data[data[:,0].argsort(kind='mergesort')]

        blocks = []
        blen = 0
        bstart = 0

        for i in range(len(data)):
            if blen < 16:
                blen += 1
            else:
                if data[i][0] == data[i-1][0]:
                    blen += 1
                else:
                    blocks.append(data[bstart:i])
                    bstart = i
                    blen = 1
        if blen != 0:
            blocks.append(data[bstart:])


        o = len(data) - (len(blocks) * 16)  # number of overflow items
        r = len(blocks) * 16                # number of regular items in blocks
        mr = int((r - 1) / 16) + 1          # number of sync blocks
        data_offset = mr*8+8                # start offset of data in compontent

        assert mr == len(blocks)
    
        print(f'Compressed Index:')
        print(f'\t{len(data)} total items')
        print(f'\t{r} regular items, {o} overflow items')
        print(f'\t{len(blocks)} sync blocks')

        packed_blocks = []
        block_keys = []

        for b in blocks:
            bo = encode_varint(len(b) - 16)

            keys = b[:16,0]
            block_keys.append(keys[0])
            keys_delta = []
            for i in range(1, len(keys)):
                keys_delta.append(keys[i] - keys[i-1])
            
            positions = b[:,1].astype(np.int64) # cpos offsets can be negative
            positions_delta = []
            for i in range(1, len(positions)):
                positions_delta.append(positions[i] - positions[i-1])

            packed = bo
            packed += b''.join(encode_varint(int(x)) for x in keys_delta)
            packed += b''.join(encode_varint(x) for x in positions_delta)

            packed_blocks.append(packed)
        
        assert mr == len(packed_blocks)
        assert mr == len(block_keys)

        offsets = [data_offset]
        for i, b in enumerate(blocks[:-1], start=1):
            offsets.append(offsets[i-1] + len(b))

        assert len(offsets) == mr and len(offsets) == len(block_keys)

        sync = []
        for k, o in zip(block_keys, offsets):
            sync.append(pack('<Q', k))
            sync.append(pack('<q', o))

        self.encoded = pack('<q', r)
        self.encoded += b''.join(sync)
        self.encoded += b''.join(packed_blocks)


    def bytelen(self):
        return len(self.encoded)


    def write(self, f):
        f.write(self.encoded)
