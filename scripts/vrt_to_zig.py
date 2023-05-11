#!/usr/bin/env python3

import argparse

from ziggypy.components import *
from ziggypy.layers import *
from ziggypy.variables import *

from pathlib import Path
from uuid import UUID

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


### VRT processing

print('Processing VRT...')

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
                    corpus[i].append((attr).encode('utf-8'))

# double check dimensions
assert all(len(p) == len(corpus[0]) for p in corpus), "P attributes of supplied VRT don't have the same dimensions"
clen = len(corpus[0])

print(f'Found input file with {clen} corpus positions')


### Datastore creation

# A datastore consists of container files, which all have a UUID v4.
# Container files can be layer files and variables assigned to them.
# A datastore is built up from the bottom beginning with a primary layer that
# provides a global index of corpus positions (cpos), its variables, and additional
# layers that can reference layers below them.
# All these containers are linked via UUIDs.

# static uuids for now to make testing easier
base_uuid = UUID('b764b867-cac4-4329-beda-9c021c5184d7') # uuid of base layer container
token_uuid = UUID('b7887880-e234-4dd0-8d6a-b8b99397b030') # uuid of first P-attr (token stream)
pos_uuid = UUID('634575cf-43c2-4a7e-b239-4e0ce2ecb394') # uuid of second P-attr (pos tags)


# partition vector:
# no partition = 1 partition spanning the entire corpus
# with boundaries (0, clen)
partitions = [0, clen]


## Datastore Objects

datastore = dict()

# Primary Layer with corpus dimensions
datastore["primary_layer"] = PrimaryLayer(clen, partitions, base_uuid)

# Plain String Variable for tokens
datastore["p_token"] = PlainStringVariable(datastore["primary_layer"], corpus[0], uuid = token_uuid, compressed = not args.uncompressed)

# Indexed String Variable for POS tags
datastore["p_pos"] = IndexedStringVariable(datastore["primary_layer"], corpus[1], uuid = pos_uuid, compressed= not args.uncompressed)

for name, obj in datastore.items():
    ztype = obj.__class__.__name__
    ext = ".zigl" if isinstance(obj, Layer) else ".zigv"
    p = args.output / (str(obj.uuid) + ext)

    print(f"Writing {ztype} '{name}' to file {p}")
    with p.open(mode = "wb") as f:
        obj.write(f)
