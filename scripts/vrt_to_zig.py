#!/usr/bin/env python3

import argparse

from pathlib import Path
from uuid import UUID
from struct import pack

from fnvhash import fnv1a_64

parser = argparse.ArgumentParser(description='Script to convert a VRT file to a ziggurat basic layer')
parser.add_argument('input', type=Path,
                    help='The VRT file to convert')
parser.add_argument('-o', type=Path, required=False, dest="output",
                    help='The output directory for the Ziggurat data store. Default is input filename without extension')
parser.add_argument('-f', '--force', action='store_true',
                    help='Force overwrite output if directory already exists')

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

clen = 0 # length of corpus, first to be determined by scanning the VRT file

with args.input.open() as f:
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                clen += 1
print(f'Found input file with {clen} corpus positions')

### Write Base Layer container

f = (args.output / (str(base_uuid) + '.zigl')).open(mode="wb")

## write header
# consts
f.write('Ziggurat'.encode('ascii')) # magic
f.write('1.0\t'.encode('ascii')) # version
f.write('Z'.encode('ascii')) # container family
f.write('L'.encode('ascii')) # container class
f.write('p'.encode('ascii')) # container type (primary layer)
f.write('\n'.encode('ascii')) # LF

f.write(str(base_uuid).encode('ascii')) # uuid as ASCII (36 bytes)
f.write('\n\x04\0\0'.encode('ascii')) # LF EOT 0 0

# components meta
f.write(pack('B', 1)) #allocated
f.write(pack('B', 1)) #used

f.write(bytes(6)) # padding

# dimensions
f.write(pack('<q', clen)) # dim1
f.write(pack('<q', 0)) # dim2

# referenced base layers
f.write(bytes(40)) # base1_uuid + padding
f.write(bytes(40)) # base2_uuid + padding

def bom_entry(type, name, mode, offset,
              size, param1, param2):
    assert len(name.encode('ascii')) <= 12
    return pack('B', 1) +\
        pack('B', type) +\
        pack('B', mode) +\
        name.encode('ascii').ljust(13) +\
        pack('<q', offset) +\
        pack('<q', size) +\
        pack('<q', param1) +\
        pack('<q', param2)

BOM_START = 160
LEN_BOM_ENTRY = 48

def data_start(clen):
    return BOM_START + (clen * LEN_BOM_ENTRY)

## write BOM entries

# Partition vector
f.write(bom_entry(
    0x4,
    'Partition',
    0x0,
    data_start(1), # offset
    16, # size
    0xABCD,
    1
))

## write components

# Partition vector (min size 2)

# no partition = 1 partition spanning the entire corpus
# with boundaries (0, clen)

f.write(pack('<q', 0))
f.write(pack('<q', clen))

f.close()

### Process VRT into variable containers
