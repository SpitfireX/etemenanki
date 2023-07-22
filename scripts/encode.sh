#! /bin/bash

./vrt_to_zig.py -f \
-p token \
-p pos \
-s s \
    -a s+id:plain \
-s text \
    -a text+title:plain \
    -a text+id:int \
    -a text+url:plain \
    -a text+author:indexed \
    -a text+date:indexed \
    -a text+yearmonth:indexed \
    -a text+year:int \
    -a text+category:indexed \
    -a text+keywords:set \
    -a text+related:set \
    -a text+ingredients:set \
"../../soupchef-cwb/output/$1"