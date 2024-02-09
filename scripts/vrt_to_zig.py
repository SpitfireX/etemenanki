#!/usr/bin/env python3

import argparse
import xml.parsers.expat as expat
import gzip
import tempfile

from ziggypy.components import *
from ziggypy.layers import *
from ziggypy.variables import *
from ziggypy.util import PFileIter

from pathlib import Path

parser = argparse.ArgumentParser(description="Script to convert a VRT file to a ziggurat basic layer")
parser.add_argument("input", type=Path,
                    help="The VRT file to convert")
parser.add_argument("-o", type=Path, required=False, dest="output",
                    help="The output directory for the Ziggurat data store. Default is input filename without extension")
parser.add_argument("-f", "--force", action="store_true",
                    help="Force overwrite output if directory already exists")
parser.add_argument("-u", "--uncompressed", action="store_true",
                    help="Write all components uncompressed (storage mode 0x00)")
parser.add_argument("-x", "--invalid-xml", action="store_true",
                    help="Fix invalid XML. Encloses whole corpus in an additional root element.")
parser.add_argument("-p", action="append", metavar="p_attribute_name", default=[],
                    help="""Declares and names a p-attribute. Order of declaration must correspond to order of columns in input.
                    P-attributes are encoded as variables on the primary layer of the corpus.
                    Variable type can be specified with a colon after the name, i.e. 'pos:indexed'.
                    Valid variable types are: indexed, plain, int, delta, set, ptr, skip.
                    The type "ptr" as of now only works for a singluar ptr attribute per encoding run and is intended to encode
                    universal dependencies style dependency relations. See "--ptr-base" below for more details. 
                    The type "skip" denotes that a column should be skipped and not encoded.""")
parser.add_argument("-s", action="append", metavar="s_attribute_name", default=[],
                    help="""Declares an s-attribute. The attribute name must correspond to the attribute's XML tag in the input.
                    S-attributes are encoded as segmentation layers and thus only store the start and end positions of the spans
                    enclosed by the XML tags.
                    e.g. '-s text'.
                    For encoding of the tag's attributes see '-a'.""")
parser.add_argument("-a", action="append", metavar="s_annotation_spec", default=[],
                    help="""Declares an annotation spec for an s-attribute annotation. In the input these annotations correspond
                    with the attributes of the s-attribute's XML tags. Annotations consist of three parts: The s-attribute's name,
                    the annotation's name, and a Ziggurat variable type. This takes the form 's_attr+name:type',
                    e.g. '-a text+url:plain'.
                    Valid variable types are: indexed, plain, int, delta, set.
                    """)
parser.add_argument("--int-default", type=int, metavar="int_default",
                    help="""The default value used when an invalid integer value is encountered while encoding an attribute.
                    If no default is given, the encoder will exit with an error (default behavior).""")
parser.add_argument("--ptr-base", type=str, metavar="ptr_base",
                    help="""This argument denotes the name of a p-attribute for use as a reference for pointer calculation in 
                    combination with the "ptr" attribute type. Said p-attribute needs to be specified with the "-p" flag and may
                    be of type "skip", but the actual data in the input file must be ints.
                    Assuming the arguments "-p index:skip -p head:ptr --ptr-base index"
                    pointers are calculated via the following formula: corpus_position + (head - index)""")

args = parser.parse_args()

p_attrs = []
for p in args.p:
    p = p.split(":", 1)
    if len(p) == 1:
        name, type = p[0], "indexed"
    else:
        name, type = p
    assert type in ("indexed", "plain", "int", "delta", "set", "ptr", "skip"), f"Invalid variable type '{type}' for p-attribute '{name}'"
    p_attrs.append((name, type))

# validation just for ptr variables
ptrcount = [p[1] for p in p_attrs].count("ptr")
assert  ptrcount <= 1, "maximum of one pointer variable can be encoded at the same time"
if ptrcount > 0:
    assert args.ptr_base, "--ptr-base must be specified for encoding pointer variables"
    try:
        next(n for (n, _) in p_attrs if n == args.ptr_base)
    except:
        raise ValueError(f"specified ptr-base '{args.ptr_base}' does not exist in specified p-attributes"
)


s_attrs = args.s

s_annos = dict()
for a in args.a:
    try:
        attr, anno = a.split("+")
        anno, type = anno.split(":")
        assert type in ("indexed", "plain", "int", "delta", "set"), f"Invalid variable type '{type}' for annotation {anno} for s-attribute '{attr}'"
        if attr not in s_annos.keys():
            s_annos[attr] = []
        s_annos[attr].append((anno, type))
    except:
        print(f"Invalid s-attribute annotation spec '{a}'")
        exit()


### VRT processing

print("Processing VRT...")

stack = [] # parsing stack for s attrs, list of openend tags (startpos, tagname, attrs)
spans = dict() # keys are the different s attrs, values list of spans (startpos, endpos)
span_attrs = dict() # same as above, but values are the associated attributes for each span 
cpos = 0
pcount = 0

# XML parser to parse s attrs
parser_state = (False, "", None) # (is_closing_tag, tagname, attributes)

def start_element(name, attrs):
    global parser_state
    parser_state = (False, name, attrs)

def end_element(name):
    global parser_state
    parser_state = (True, name, None)

parser = expat.ParserCreate()
if ".xml" not in str(args.input) or args.fix_xml:
    parser.Parse("<xml-fix-pseudo-start>") # init with one global start tag to keep parser happy
parser.StartElementHandler = start_element
parser.EndElementHandler = end_element

def open_input():
    if args.input.suffix == ".gz":
        return gzip.open(args.input, mode = "rt")
    else:
        return args.input.open()

with open_input() as f:
    # find number of p attrs
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                pcount = len(line.split())
                break
    
    print(f"\t found {pcount} p-attrs in input")
    assert len(p_attrs) <= pcount, "Not enough columns for specified p-attrs in input"
    f.seek(0) # reset file to beginning

    for line in f:
        # p attrs
        if not line.startswith("<"):
            if line.strip():
                cpos += 1

        # s attrs
        else:
            parser.Parse(line)
            is_closing_tag, tagname, attrs = parser_state

            if tagname in s_attrs:
                if not is_closing_tag:
                    stack.append((cpos, tagname, attrs))
                else:
                    if len(stack) > 0 and stack[-1][1] == tagname:
                        startpos, start_tagname, attrs = stack.pop()
                        if tagname not in spans.keys():
                            spans[tagname] = []
                            span_attrs[tagname] = []
                        spans[tagname].append((startpos, cpos))
                        span_attrs[tagname].append(attrs)

print(f"\t found {len(spans.keys())} s-attrs: {tuple(spans.keys())}")

print("Encoding the following attributes:")
for name, type in p_attrs:
    if type == "ptr":
        print(f"\tp-attribute '{name}' of type '{type}' with base '{args.ptr_base}'")
    elif type != "skip":
        print(f"\tp-attribute '{name}' of type '{type}'")
for name in s_attrs:
    print(f"\ts-attribute '{name}'", end="")
    if name in s_annos.keys() and len(s_annos[name]) > 0:
        print(f"with annotation {'s' if len(s_annos[name]) > 1 else ''}")
    else:
        print()
    if name in s_annos.keys():
        for annotation, type in s_annos[name]:
            print(f"\t\t'{annotation}' of type '{type}'")

if not p_attrs and not s_attrs and not s_annos:
    print("\tNo attributes for encoding")
    print("Hint: you should probably specify some with -p, -s, or -a (see --help)")
    exit()

clen = cpos

print(f"Input corpus has {clen} corpus positions")


# output file handling

if not args.output:
    args.output = Path(args.input.stem)

if args.output.exists() and not args.force:
    print(f"Output directory {args.output} exists, aborting.")
    exit()
else:
    print(f"Using output directory {args.output}")
    if not args.output.exists():
        args.output.mkdir()


### Datastore creation
print("Building Ziggurat datastore...")

# A datastore consists of container files, which all have a UUID v4.
# Container files can be layer files and variables assigned to them.
# A datastore is built up from the bottom beginning with a primary layer that
# provides a global index of corpus positions (cpos), its variables, and additional
# layers that can reference layers below them.
# All these containers are linked via UUIDs.


def write_datastore_object(obj, filename):
    ztype = obj.__class__.__name__
    ext = ".zigl" if isinstance(obj, Layer) else ".zigv"
    p = args.output / (filename + ext)
    p.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing {ztype} '{filename}' to file {p}")
    with p.open(mode = "wb") as f:
        obj.write(f)


def parse_set(str):
    return set(s.encode("utf-8") for s in str.strip().split("|") if s)

def parse_int(str, default=None):
    try:
        return int(str)
    except Exception as e:
        if default is None:
            raise e
        else:
            return default

def parse_ptr(cpos: int, base: str, head: str):
    try:
        h = int(head)
        if h == 0:
            return cpos
        else:
            b = int(base)
            return cpos + (h - b)
    except:
        return -1


## Primary Layer with corpus dimensions
primary_layer = PrimaryLayer(clen, comment = f"{args.input.name}")
write_datastore_object(primary_layer, "primary")


## Primary Layer Variables for p attributes

for i, (name, type) in enumerate(p_attrs):
    c = f"p-attr {name}"

    with open_input() as f:
        fileiter = PFileIter(f, i)

        # try:
        if type == "indexed":
            variable = FileIndexedStringVariable(primary_layer, fileiter, compressed = not args.uncompressed, comment = c)
        elif type == "plain":
            variable = PlainStringVariable(primary_layer, fileiter, compressed = not args.uncompressed, comment = c)
        elif type == "int":
            variable = IntegerVariable(primary_layer, [parse_int(s, default=args.int_default) for s in fileiter], compressed = not args.uncompressed, comment = c)
        elif type == "delta":
            variable = IntegerVariable(primary_layer, [parse_int(s, default=args.int_default) for s in fileiter], compressed = not args.uncompressed, delta= True, comment = c)
        elif type == "set":
            variable = SetVariable(primary_layer, [parse_set(s) for s in fileiter], comment = c)
        elif type == "ptr":
            base_index = next(i for i, (n, _) in enumerate(p_attrs) if n == args.ptr_base)
            with open_input() as f2:
                base = PFileIter(f2, base_index, len(p_attrs))
                variable = PointerVariable(primary_layer, [parse_ptr(cpos, b, h) for cpos, (b, h) in enumerate(zip(base, fileiter))], compressed = not args.uncompressed, comment = c)
        elif type == "skip":
            continue
        else:
            print(f"Invalid type '{type}' for p attribute '{name}'")
            continue
        # except Exception as e:
        #     print(f"Error while encoding p attribute '{name}': {e}")
        #     exit()

    write_datastore_object(variable, name)


s_attr_layers = dict()

## Segmentation Layers for s attributes

for attr in s_attrs:
    slen = len(spans[attr])
    layer = SegmentationLayer(primary_layer, slen, spans[attr], compressed = not args.uncompressed, comment = f"s-attr {attr}")

    s_attr_layers[attr] = layer
    write_datastore_object(layer, f"{attr}/{attr}")


## Variables for s attribute annotations

for attr, annos in s_annos.items():
    base_layer = s_attr_layers[attr]

    for anno, type in annos:

        data = [attrs[anno] for attrs in span_attrs[attr]]
        assert len(data) == base_layer.n, f"Inconsistend number of annotations for annotation '{anno}' for s attribute '{attr}'"

        c = f"s-attr {attr}_{anno}"

        try:
            if type == "indexed":
                variable = IndexedStringVariable(base_layer, [s.encode("utf-8") for s in data], compressed = not args.uncompressed, comment = c)
            elif type == "plain":
                variable = PlainStringVariable(base_layer, data, compressed = not args.uncompressed, comment = c)
            elif type == "int":
                variable = IntegerVariable(base_layer, [parse_int(s, default=args.int_default) for s in data], compressed = not args.uncompressed, comment = c)
            elif type == "delta":
                variable = IntegerVariable(base_layer, [parse_int(s, default=args.int_default) for s in data], compressed = not args.uncompressed, delta=True, comment = c)
            elif type == "set":
                variable = SetVariable(base_layer, [parse_set(s) for s in data], comment = c)
            else:
                print(f"Invalid type '{type}' for annotation '{anno}' of s attribute '{attr}'")
                continue
        except Exception as e:
            print(f"Error while encoding annotation {anno} for s attribute '{attr}': {e}")
            exit()

        write_datastore_object(variable, f"{attr}/{anno}")
