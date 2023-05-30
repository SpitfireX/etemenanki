# Etemenanki

A highly experimental implementation of the [Ziggurat](https://cwb.sourceforge.io/ziggurat.php) corpus data model.

As of now this project is an experimentation sandbox for both the in-memory data structures and the storage representation of Ziggurat.

## Components in this repo

- etemenanki: A Rust library for reading Ziggurat datastores
- vrt_to_zig.py: A Python script for encoting VRT files to Ziggurat datastores
- ziggurat-varint: A combined Rust crate and Python module implementing the Ziggurat varint format
- ZiggyPy: A Python module for interacting with (as of now: only writing) Ziggurat datastores.
