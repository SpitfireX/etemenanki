import numpy as np

try:
    from ziggurat_varint import *
except ImportError:
    print("Warning: Using slow VarInt implementation, consider installing the faster ziggurat_varint module")
    from .varint import encode_varint
    encode_varint_unsigned = encode_varint
    encode_varint_block = lambda block: b"".join(encode_varint(i) for i in block)
    encode_varint_block_unsigned = encode_varint_block

from .util import batched

from abc import ABC, abstractmethod
from typing import Tuple, Optional, Iterable, Any, Sequence
from io import RawIOBase
from struct import pack
from itertools import islice

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
    
    def __init__(self, items: Iterable[Any], name: str, n: int, d: int = 1):
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


class VectorComp(Component):

    def __init__(self, items: Iterable[Any], name: str, n: int, d: int = 1):
        super().__init__(
            0x04,
            0x01,
            name,
            (n, d)
        )        
        self.n = n
        self.d = d
        data = np.atleast_2d(np.array(items, dtype=np.int64))
        data.shape = (n, d)

        # compress data

        m = int((n - 1) / BLOCKSIZE) + 1
        comp_start = m*8

        # VarInt encoded blocks
        blocks = []

        for i in range(0, n, BLOCKSIZE):
            remaining = data.shape[0] - i
            if remaining >= 16:
                block = data[i : i+BLOCKSIZE]
            else:
                block = np.full((16, d), -1, dtype=np.int64)
                block[:remaining] = data[i:]
            
            cols = []

            for j in range(d):
                row = block[:, j]
                cols.append(encode_varint_block(row))

            blocks.append(b''.join(cols))

        assert len(blocks) == m

        # Sync offsets
        sync = [comp_start]
        for i, b in enumerate(blocks[:-1], start=1):
            sync.append(sync[i-1] + len(b))
        
        assert len(sync) == m

        self.encoded = b''.join(pack('<q', s) for s in sync) +\
            b''.join(blocks)


    def bytelen(self):
        return len(self.encoded)

    
    def write(self, f):
        f.write(self.encoded)          


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
            remaining = data.shape[0] - i
            if remaining >= 16:
                block = data[i : i+BLOCKSIZE]
            else:
                block = np.full((16, d), -1, dtype=np.int64)
                block[:remaining] = data[i:]
            
            delta = np.empty(block.shape, dtype=np.int64)
            delta[0] = np.copy(block[0])

            cols = []

            for j in range(d):
                for i in range(1, len(block)):
                    delta[i][j] = block[i][j] - block[i-1][j]

                row = delta[:, j]
                cols.append(encode_varint_block(row))

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

        self.encoded = bytearray()

        for s in islice(strings, n):
            self.encoded.extend(s)
            self.encoded.extend(b'\0')
        
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


class StringVector(Component):

    def __init__(self, strings: Iterable[bytes], name: str, n: int):
        """strings: series of utf-8 encoded null terminated strings"""

        self.encoded = bytearray()
        self.offsets = []
        
        offset = 0
        for s in islice(strings, n):
            self.encoded.extend(s)
            self.encoded.extend(b'\0')
            self.offsets.append(offset)
            offset += len(s)+1

        super().__init__(
            0x03,
            0x00,
            name,
            (n, 0)
        )


    def bytelen(self):
        return len(self.offsets)*8 + len(self.encoded)


    def write(self, f):
        f.write(b''.join(pack('<q', o) for o in self.offsets))
        f.write(self.encoded)


class Set(Component):

    def __init__(self, sets: Iterable[Sequence[int]], name: str, n: int):

        super().__init__(
            0x05,
            0x01,
            name,
            (n, 2)
        )

        blocks = []

        # group sets into blocks of 16
        for batch in batched(sets, 16):
            offsets = []
            lengths = []
            encoded_items = b""

            # delta encode each set
            itemoffset = 0
            for set in batch:
                set = list(set)
                delta = [set[0]] if len(set) > 0 else []
                for i in range(1, len(set)):
                    delta.append(set[i] - set[i-1])
                
                encoded = encode_varint_block(delta)
                
                offsets.append(itemoffset)
                lengths.append(len(encoded))
                encoded_items += encoded

                itemoffset += len(encoded)

            # pad arrays
            if len(offsets) < 16:
                padding = 16 - len(offsets)
                offsets.extend([-1] * padding)
                lengths.extend([0] * padding)

            # delta compress offset array

            offsets_delta = [offsets[0]]
            for i in range(1, len(offsets)):
                offsets_delta.append(offsets[i] - offsets[i-1])

            # assemble block
            block = encode_varint_block(offsets_delta)
            block += encode_varint_block(lengths)
            block += encoded_items

            blocks.append(block)
        
        # synchronisation vector with offsets for each block
        sync = [0]
        for i, b in enumerate(blocks[:-1], start=1):
            sync.append(sync[i-1] + len(b))

        self.sync = sync
        self.blocks = blocks        

    
    def bytelen(self):
        return len(self.sync)*8 + sum(len(b) for b in self.blocks)

    
    def write(self, f):
        for o in self.sync:
            f.write(pack('<q', o))
        for b in self.blocks:
            f.write(b)


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
        block_padding = 0

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
            if blen < 16: # padding to a full block
                block_padding = 16 - blen
                block = np.full((16, 2), -1, dtype=np.uint64)
                block[:blen] = data[bstart:]
                blocks.append(block)
            else:
                blocks.append(data[bstart:])


        r = len(blocks) * 16 - block_padding   # number of regular items in blocks
        o = len(data) - r                      # number of overflow items
        mr = int((r - 1) / 16) + 1             # number of sync blocks
        data_offset = mr*8+8                   # start offset of data in compontent

        assert mr == len(blocks)
    
        print(f'Compressed Index:')
        print(f'\t{len(data)} total items')
        print(f'\t{r} regular items, {o} overflow items')
        print(f'\t{len(blocks)} sync blocks')

        packed_blocks = []
        block_keys = []

        for b in blocks:

            keys = b[:16,0]
            block_keys.append(keys[0])
            keys_delta = [int(keys[0])]
            for i in range(1, len(keys)):
                keys_delta.append(int(keys[i] - keys[i-1]))
            
            positions = b[:,1].astype(np.int64) # cpos offsets can be negative
            positions_delta = [positions[0]]
            for i in range(1, len(positions)):
                positions_delta.append(positions[i] - positions[i-1])

            packed = encode_varint(len(b) - 16)
            packed += encode_varint_block_unsigned(keys_delta)
            packed += encode_varint_block(positions_delta)

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


class InvertedIndex(Component):

    def __init__(self, types: Sequence[Any], positions: Iterable[Iterable[int]], name: str, k: int, p: int):
        """positions: sequence of lists of lexicon positions for each corpus position"""

        assert p == 0, "jump tables for inverted index are not implemented yet"

        super().__init__(
            0x07,
            0x01,
            name,
            (k, p)
        )

        postings = [[] for _ in types]
        for i, occurences in enumerate(positions):
            for t in occurences:
                postings[t].append(i)


        postings_encoded = []

        for pl in postings:

            delta = np.array(pl, dtype=np.int64)
            sub = np.append(np.array([0], dtype=np.int64), delta[1:])
            delta -= sub

            block = b"\x00" # jump table offset for now always zero TODO
            block += encode_varint_block(delta)
                
            postings_encoded.append(block)

        self.encoded = b''

        offset = k * 16
        for t, e in zip(postings, postings_encoded):
            self.encoded += pack('<q', len(t)) # type frequency
            self.encoded += pack('<q', offset) # offset for postings list
            offset += len(e)

        self.encoded += b''.join(postings_encoded)


    def bytelen(self):
        return len(self.encoded)


    def write(self, f):
        f.write(self.encoded)
