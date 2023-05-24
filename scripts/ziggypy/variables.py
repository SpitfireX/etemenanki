from abc import ABC
from io import RawIOBase
from itertools import chain, accumulate
from uuid import UUID, uuid4
from collections import Counter

from .container import Container
from .components import *
from .layers import Layer

from fnvhash import fnv1a_64

class Variable(ABC):

    def __init__(self, base_layer: Layer, uuid: UUID):
        self.base_layer = base_layer
        self.uuid = uuid


    def write(self, f: RawIOBase):
        self.container.write(f)


class PlainStringVariable(Variable):

    def __init__(self, base_layer: Layer, strings: Iterable[bytes], uuid: Optional[UUID] = None, compressed: bool = True):
        
        super().__init__(base_layer, uuid if uuid else uuid4())

        # build StringData [string]
        print('Building StringData')

        string_data = StringList(strings, 'StringData', base_layer.n)

        # build OffsetStream [offset_to_next_string]
        print('Building OffsetStream')
        offset_stream = list(accumulate(chain([0], strings), lambda x, y: x + len(y)))

        if compressed:
            offset_stream = VectorDelta(offset_stream, 'OffsetStream', len(offset_stream))
        else:
            offset_stream = Vector(offset_stream, 'OffsetStream', len(offset_stream))


        # build StringHash [(hash, cpos)]
        print('Building StringHash')
        string_pairs = [(fnv1a_64(s), i) for i, s in enumerate(strings)]

        if compressed:
            string_hash = IndexCompressed(string_pairs, "StringHash", base_layer.n)
        else:
            string_hash = Index(string_pairs, "StringHash", base_layer.n)

        self.container = Container((string_data, offset_stream, string_hash),
            'ZVc',
            (base_layer.n, 0),
            self.uuid,
            (base_layer.uuid, None)
        )


class IndexedStringVariable(Variable):

    def __init__(self, base_layer: Layer, strings: Iterable[bytes], uuid: Optional[UUID] = None, compressed: bool = True):
        
        super().__init__(base_layer, uuid if uuid else uuid4())

        # lexicon of unique strings, sorted by total occurence
        lex = Counter(strings)
        lex = [x[0] for x in lex.most_common()]

        lsize = len(lex)
        print("lexicon size:", lsize)

        lexicon = StringVector(lex, "Lexicon", lsize)

        # lexicon hashes
        hashes = [(fnv1a_64(l), i) for i, l in enumerate(lex)]

        lexindex = Index(hashes, "LexHash", lsize)

        lexids = [lex.index(pos) for pos in strings]

        if compressed:
            lexidstream = VectorComp(lexids, "LexIDStream", len(lexids))
        else:
            lexidstream = Vector(lexids, "LexIDStream", len(lexids))

        # inverted lookup index associating each lexicon ID with its positionso of occurence
        invidx = InvertedIndex(lex, lexids, "LexIDIndex", lsize, 0)

        p_vec = Vector(self.base_layer.partition, 'Partition', len(self.base_layer.partition))

        self.container = Container(
            (lexicon, lexindex, p_vec, lexidstream, invidx),
            'ZVx',
            (self.base_layer.n, lsize),
            self.uuid,
            (base_layer.uuid, None)
        )


class IntegerVariable(Variable):

    def __init__(self, base_layer: Layer, ints: Sequence[int], b: int = 1, uuid: Optional[UUID] = None, compressed: bool = True, delta: bool = False):
    
        super().__init__(base_layer, uuid if uuid else uuid4())

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
            (base_layer.uuid, None)
        )
