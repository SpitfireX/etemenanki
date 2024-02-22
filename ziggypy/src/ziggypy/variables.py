from abc import ABC
from io import RawIOBase
from itertools import chain, accumulate
from uuid import UUID, uuid4
from collections import Counter
from typing import Callable
from os.path import realpath

from ziggypy.util import ResettableIter
from ziggypy._rustypy import encode_indexed_from_p, encode_int_from_p, encode_int_from_a, encode_ptr_from_p

from .container import Container
from .components import *
from .layers import Layer

from ctypes import c_int64

from fnvhash import fnv1a_64

def fnv_hash(data: bytes) -> int:
    return c_int64(fnv1a_64(data)).value

class Variable(ABC):

    def __init__(self, base_layer: Layer, uuid: UUID):
        self.base_layer = base_layer
        self.uuid = uuid


    def write(self, f: RawIOBase):
        self.container.write(f)


class PlainStringVariable(Variable):

    def __init__(self, base_layer: Layer, strings: Iterable[str], uuid: Optional[UUID] = None, compressed: bool = True, comment: str = ""):
        
        super().__init__(base_layer, uuid if uuid else uuid4())

        # build StringData [string]
        print('Building StringData')

        string_data = StringList((s.encode("utf-8") for s in strings), 'StringData', base_layer.n)
        assert len(string_data) == base_layer.n, "variable must be of same size as its base layer"

        # build OffsetStream [offset_to_next_string]
        print('Building OffsetStream')
        offset_stream = list(accumulate(chain([0], string_data.strings()), lambda x, y: x + len(y) + 1))

        if compressed:
            offset_stream = VectorDelta(offset_stream, 'OffsetStream', len(offset_stream))
        else:
            offset_stream = Vector(offset_stream, 'OffsetStream', len(offset_stream))


        # build StringHash [(hash, cpos)]
        print('Building StringHash')
        string_pairs = [(fnv_hash(s), i) for i, s in enumerate(string_data.strings())]

        if compressed:
            string_hash = IndexCompressed(string_pairs, "StringHash", base_layer.n)
        else:
            string_hash = Index(string_pairs, "StringHash", base_layer.n)

        self.container = Container(
            (string_data, offset_stream, string_hash),
            'ZVc',
            (base_layer.n, 0),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )


class IndexedStringVariable(Variable):

    def __init__(self, base_layer: Layer, strings: Sequence[bytes], uuid: Optional[UUID] = None, compressed: bool = True, comment: str = ""):
        
        super().__init__(base_layer, uuid if uuid else uuid4())

        assert len(strings) == base_layer.n, "variable must be of same size as its base layer"

        # lexicon of unique strings, sorted by total occurence
        lex = Counter(strings)
        total = lex.total()
        assert total == len(strings), "lexicon dropped tokens"
        lex = {x[0]: i for i, x in enumerate(lex.most_common())}


        lsize = len(lex)
        print("lexicon size:", lsize)

        lexicon = StringVector(lex.keys(), "Lexicon", lsize)

        # lexicon hashes
        hashes = [(fnv_hash(l), i) for l, i in lex.items()]

        lexindex = Index(hashes, "LexHash", lsize)

        lexids = (lex[s] for s in strings)

        if compressed:
            lexidstream = VectorComp(lexids, "LexIDStream", total)
        else:
            lexidstream = Vector(lexids, "LexIDStream", total)

        # inverted lookup index associating each lexicon ID with its positionso of occurence
        invidx = InvertedIndex(list(lex), lexids, "LexIDIndex", lsize)

        self.container = Container(
            (lexicon, lexindex, lexidstream, invidx),
            'ZVx',
            (self.base_layer.n, lsize),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )

class RustyIndexedStringVariable:
    def __init__(self, base_layer: Layer, file: RawIOBase, src: int | tuple[str, str], length: int, uuid: Optional[UUID] = None, compressed: bool = True, comment: str = ""):
        self.base = str(base_layer.uuid)
        self.input = realpath(file.name)
        self.src = src
        self.length = length
        self.compressed = compressed
        self.comment = comment

    def write(self, f: RawIOBase):
        output = realpath(f.name)

        if type(self.src) is int:
            encode_indexed_from_p(self.input, self.src, self.length, self.base, self.compressed, self.comment, output)
        elif type(self.src) is tuple and len(self.src) == 2 and type(self.src[0]) is str and type(self.src[1]) is str:
            tag, attr = self.src
            # encode_int_from_a(self.input, tag, attr, self.length, self.default, self.base, self.compressed, self.delta, self.comment, output)
        else:
            raise TypeError("wrong type for src, must be int or (str, str)")


class FileIndexedStringVariable(Variable):
    """Hacky copy pasted code to allow indexing without keeping all the tokens in RAM.
    All of this needs to be thrown away and implemented proprely at some time actually using the proper
    Ziggurat data structures."""

    def __init__(self, base_layer: Layer, file: ResettableIter, uuid: Optional[UUID] = None, compressed: bool = True, comment: str = ""):
        
        super().__init__(base_layer, uuid if uuid else uuid4())

        size = base_layer.n

        # lexicon of unique strings, sorted by total occurence
        strings = (line.strip().encode("utf-8") for line in file)
        lex = Counter(strings)
        lex = {x[0]: i for i, x in enumerate(lex.most_common())}

        lsize = len(lex)
        print("lexicon size:", lsize)

        lexicon = StringVector(lex.keys(), "Lexicon", lsize)

        # lexicon hashes
        hashes = [(fnv_hash(l), i) for l, i in lex.items()]

        lexindex = Index(hashes, "LexHash", lsize)

        file.reset()
        strings = (line.strip().encode("utf-8") for line in file)
        lexids = (lex[s] for s in strings)

        if compressed:
            lexidstream = VectorComp(lexids, "LexIDStream", size)
        else:
            lexidstream = Vector(lexids, "LexIDStream", size)

        # inverted lookup index associating each lexicon ID with its positions of occurence
        file.reset()
        strings = (line.strip().encode("utf-8") for line in file)
        lexids = ((lex[s],) for s in strings)
        
        invidx = InvertedIndex(list(lex), lexids, "LexIDIndex", lsize)

        self.container = Container(
            (lexicon, lexindex, lexidstream, invidx),
            'ZVx',
            (self.base_layer.n, lsize),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )


class IntegerVariable(Variable):

    def __init__(self, base_layer: Layer, ints: Sequence[int], b: int = 1, uuid: Optional[UUID] = None, compressed: bool = True, delta: bool = False, comment: str = ""):
    
        super().__init__(base_layer, uuid if uuid else uuid4())

        assert len(ints) == base_layer.n, "variable must be of same size as its base layer"

        # stream of integers

        if compressed:
            if delta:
                int_stream = VectorDelta(ints, "IntStream", len(ints))
            else:
                int_stream = VectorComp(ints, "IntStream", len(ints))
        else:
            int_stream = Vector(ints, "IntStream", len(ints))

        # sort index

        pairs = [(n, i) for i, n in enumerate(ints)]
        pairs.sort(key = lambda x: x[0])

        if compressed:
            int_sort = IndexCompressed(pairs, "IntSort", len(ints))
        else:
            int_sort = Index(pairs, "IntSort", len(ints))
            

        self.container = Container(
            (int_stream, int_sort),
            'ZVi',
            (self.base_layer.n, b),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )


class FileIntegerVariable(Variable):

    def __init__(self, base_layer: Layer, file: ResettableIter, length: int, b: int = 1, uuid: Optional[UUID] = None, compressed: bool = True, delta: bool = False, comment: str = "", parse_int=int):
    
        super().__init__(base_layer, uuid if uuid else uuid4())

        assert length == base_layer.n, "variable must be of same size as its base layer"

        # stream of integers
        file.reset()
        ints = (parse_int(i) for i in file)

        if compressed:
            if delta:
                int_stream = VectorDelta(ints, "IntStream", length)
            else:
                int_stream = VectorComp(ints, "IntStream", length)
        else:
            int_stream = Vector(ints, "IntStream", length)

        # sort index
        file.reset()
        ints = (parse_int(i) for i in file)

        pairs = [(n, i) for i, n in enumerate(ints)]
        pairs.sort(key = lambda x: x[0])

        if compressed:
            int_sort = IndexCompressed(pairs, "IntSort", length)
        else:
            int_sort = Index(pairs, "IntSort", length)

        self.container = Container(
            (int_stream, int_sort),
            'ZVi',
            (self.base_layer.n, b),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )


class RustyIntegerVariable:

    def __init__(self, base_layer: Layer, file: RawIOBase, src: int | tuple[str, str], length: int, default: int = 0, uuid: Optional[UUID] = None, compressed: bool = True, delta: bool = False, comment: str = ""):
        self.base = str(base_layer.uuid)
        self.input = realpath(file.name)
        self.src = src
        self.length = length
        self.default = default
        self.compressed = compressed
        self.delta = delta
        self.comment = comment

    def write(self, f: RawIOBase):
        output = realpath(f.name)

        if type(self.src) is int:
            encode_int_from_p(self.input, self.src, self.length, self.default, self.base, self.compressed, self.delta, self.comment, output)
        elif type(self.src) is tuple and len(self.src) == 2 and type(self.src[0]) is str and type(self.src[1]) is str:
            tag, attr = self.src
            encode_int_from_a(self.input, tag, attr, self.length, self.default, self.base, self.compressed, self.delta, self.comment, output)
        else:
            raise TypeError("wrong type for src, must be int or (str, str)")


class SetVariable(Variable):
    def __init__(self, base_layer: Layer, sets: Sequence[set[bytes]], uuid: Optional[UUID] = None, comment: str = ""):

        super().__init__(base_layer, uuid if uuid else uuid4())

        assert len(sets) == base_layer.n, "variable must be of same size as its base layer"

        # global lexicon of types 
        types = Counter()
        for set in sets:
            types.update(set)
        types = {x[0]: i for i, x in enumerate(types.most_common())}

        n = len(sets) # number of sets
        assert n == base_layer.n, "Mismatch between number of sets in Set Variable and positions in the base layer"
        v = len(types.keys()) # number of unique types
        lexicon = StringVector(types.keys(), "Lexicon", v)
        
        # sort index of types
        types_hash = [(fnv_hash(t), i) for t, i in types.items()]

        lexhash = Index(types_hash, "LexHash", len(types_hash))

        # sets of type ids
        id_sets = [ sorted([types[i] for i in s]) for s in sets ]
        
        id_set_stream = Set(id_sets, "IDSetStream", n, 1)

        # index of type occurrences in sets, associates types with set IDs/layer positions
        id_set_index = InvertedIndex(list(types), id_sets, "IDSetIndex", v)

        self.container = Container(
            (
                lexicon,
                lexhash,
                id_set_stream,
                id_set_index,
            ),
            'ZVs',
            (n, v),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )


class FileSetVariable(Variable):
    def __init__(self, base_layer: Layer, file: ResettableIter, length: int, parse_set: Callable[[str], set], uuid: Optional[UUID] = None, comment: str = ""):

        super().__init__(base_layer, uuid if uuid else uuid4())

        assert length == base_layer.n, "variable must be of same size as its base layer"

        # global lexicon of types 
        file.reset()
        sets = (parse_set(l) for l in file)

        types = Counter()
        for set in sets:
            types.update(set)
        types = {x[0]: i for i, x in enumerate(types.most_common())}

        v = len(types.keys()) # number of unique types
        lexicon = StringVector(types.keys(), "Lexicon", v)

        # sort index of types
        types_hash = [(fnv_hash(t), i) for t, i in types.items()]

        lexhash = Index(types_hash, "LexHash", len(types_hash))

        # sets of type ids
        file.reset()
        sets = (parse_set(l) for l in file)
        id_sets = [ sorted([types[i] for i in s]) for s in sets ]

        id_set_stream = Set(id_sets, "IDSetStream", length, 1)

        # index of type occurrences in sets, associates types with set IDs/layer positions
        id_set_index = InvertedIndex(list(types), id_sets, "IDSetIndex", v)

        self.container = Container(
            (
                lexicon,
                lexhash,
                id_set_stream,
                id_set_index,
            ),
            'ZVs',
            (length, v),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )


class PointerVariable(Variable):
    def __init__(self, base_layer: Layer, heads: Sequence[int], uuid: Optional[UUID] = None, compressed: bool = True, comment: str = ""):

        super().__init__(base_layer, uuid if uuid else uuid4())

        assert len(heads) == base_layer.n, "variable must be of same size as its base layer"
        assert all(h >= -1 and h < len(heads) for h in heads), "Head pointers must fall into the range [-1, N-1]"

        # stream of heads
        head_stream = VectorDelta(heads, "HeadStream", len(heads))

        # sort index

        pairs = [(n, i) for i, n in enumerate(heads)]
        pairs.sort(key = lambda x: x[0])

        if compressed:
            head_sort = IndexCompressed(pairs, "HeadSort", len(heads))
        else:
            head_sort = Index(pairs, "HeadSort", len(heads))
        
        self.container = Container(
            (
                head_stream,
                head_sort,
            ),
            "ZVp",
            (len(heads), 0),
            self.uuid,
            (base_layer.uuid, None),
            comment,
        )

class RustyPointerVariable:
    def __init__(self, base_layer: Layer, file: RawIOBase, basecol: int, headcol: int, length: int, uuid: Optional[UUID] = None, compressed: bool = True, comment: str = ""):
        self.base = str(base_layer.uuid)
        self.input = realpath(file.name)
        self.basecol = basecol
        self.headcol = headcol
        self.length = length
        self.compressed = compressed
        self.comment = comment

    def write(self, f: RawIOBase):
        output = realpath(f.name)
        encodedlen = encode_ptr_from_p(self.input, self.basecol, self.headcol, self.length, self.base, self.compressed, self.comment, output)
        assert encodedlen == self.length, "discrepancy between specified and actual encoded len"
