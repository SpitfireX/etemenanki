#!/usr/bin/env python3

import argparse

from ziggypy.varint import encode_varint
from ziggypy.container import Container
from ziggypy.components import *

from pathlib import Path
from uuid import UUID
from struct import pack
from itertools import chain, accumulate, islice, groupby

from fnvhash import fnv1a_64

def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch

parser = argparse.ArgumentParser(description='Script to convert a VRT file to a ziggurat basic layer')
parser.add_argument('input', type=Path,
                    help='The VRT file to convert')
parser.add_argument('-o', type=Path, required=False, dest="output",
                    help='The output directory for the Ziggurat data store. Default is input filename without extension')
parser.add_argument('-f', '--force', action='store_true',
                    help='Force overwrite output if directory already exists')
parser.add_argument('-u', '--uncompressed', action='store_true',
                    help='Write all components uncompressed (storage mode 0x00)')

args = parser.parse_args()

# output file handling

if not args.output:
    args.output = Path(args.input.stem)

if args.output.exists() and not args.force:
    print(f"Output directory {args.output} exists, aborting.")
    exit()
else:
    if not args.output.exists():
        args.output.mkdir()

# A datastore consists of container files, which all have a UUID v4.
# Container files can be layer files and variables assigned to them.
# A datastore is built up from the bottom beginning with a primary layer that
# provides a global index of corpus positions (cpos), its variables, and additional
# layers that can reference layers below them.
# All these containers are linked via UUIDs.

# static uuids for now to make testing easier
base_uuid = UUID('b764b867-cac4-4329-beda-9c021c5184d7') # uuid of base layer container
tok_uuid = UUID('b7887880-e234-4dd0-8d6a-b8b99397b030') # uuid of first P-attr (token stream)
pos_uuid = UUID('634575cf-43c2-4a7e-b239-4e0ce2ecb394') # uuid of second P-attr (pos tags)


### Scan VRT file for number of tokens
print('Scanning VRT file...')
clen = 0 # length of corpus, first to be determined by scanning the VRT file

with args.input.open() as f:
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                clen += 1
print(f'Found input file with {clen} corpus positions')


### Write Base Layer container
p = args.output / (str(base_uuid) + '.zigl')
print(f'Writing Base Layer file {p}')

# partition vector:
# no partition = 1 partition spanning the entire corpus
# with boundaries (0, clen)
partitions = [0, clen]

p_vec = Vector(partitions, 'Partition', len(partitions))

primary_layer = Container(
    (p_vec,),
    'ZLp',
    (clen, 0),
    base_uuid
)

with p.open(mode="wb") as f:
    primary_layer.write(f)


### Process VRT

print('Processing VRT...')

## gather data

corpus = []
pcount = 0

with args.input.open() as f:
    # find number of p attrs
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                pcount = len(line.split())
                break
    
    print(f'Corpus has {pcount} p-attrs')
    corpus = [[] for _ in range(pcount)]
    f.seek(0) # reset file to beginning

    for line in f:
        if not line.startswith("<"):
            if line.strip():
                pattrs = line.split()
                for i, attr in enumerate(pattrs):
                    corpus[i].append((attr + '\0').encode('utf-8'))

# double check dimensions
for attr in corpus:
    assert len(attr) == clen

## data structures for Plain String Variable for tokens

# build StringData [string]
print('Building StringData')

string_data = StringList(corpus[0], 'StringData', clen)

# build OffsetStream [offset_to_next_string]
print('Building OffsetStream')
offset_stream = list(accumulate(chain([0], corpus[0]), lambda x, y: x + len(y)))

if args.uncompressed:
    offset_stream = Vector(offset_stream, 'OffsetStream', len(offset_stream))
else:
    offset_stream = VectorDelta(offset_stream, 'OffsetStream', len(offset_stream))


# build StringHash [(hash, cpos)]
print('Building StringHash')
string_pairs = [(fnv1a_64(s), i) for i, s in enumerate(corpus[0])]

if args.uncompressed:
    string_hash = Index(string_pairs, "StringHash", clen)
else:
    string_hash = IndexCompressed(string_pairs, "StringHash", clen)

## data structures for Indexed String Variable for POS tags

pos_lex = set(corpus[1])


### write PlainString variable container for Tokens
p = args.output / (str(tok_uuid) + '.zigv')
print(f'Writing Plain String Layer file {p}')

token_layer = Container((string_data, offset_stream, string_hash),
    'ZVc',
    (clen, 0),
    tok_uuid,
    (base_uuid, None)
)

with p.open(mode="wb") as f:
    token_layer.write(f)
