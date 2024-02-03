#! /bin/bash

../../scripts/vrt_to_zig.py -f \
-o simpledickens \
-p word \
-p pos \
-p lemma \
-s s \
-s p \
-s titlepage \
-s text \
    -a text+id:plain \
-s novel \
    -a novel+title:plain \
-s chapter \
    -a chapter+num:int \
    -a chapter+title:plain \
"$1"
