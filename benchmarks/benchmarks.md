# Ziggurat Benchmarking

## Rationale and Technical Concept

The Ziggurat data model has so far only existed as a set of ideas and specifications without an implementation. While some aspects of the data model have already been verified in purpose-built micro-benchmarks, these tests have been limited and the overall approach has not yet been tested in more comprehensive usage scenarios.
The overall main goal of the Ziggurat project is to build a technically more capable replacement for the old CWB3 data model and its storage backend. To ensure achieving this goal the following aspects need to be verified:

- elimniation of old bottlenecks in the CWB3 code (mostly related to data decoding)
- no significant new bottlenecks in the Etemenanki code and efficiency of the implemented caching strategy
- no performance regressions in common access patterns compared to CWB3 (or preferrably: performance improvements)
- scalability of Ziggurat beyond CWB3's technical limits (i.e. corpus size > 2 gigatokens)

Etemenanki aims to be the first reference implementation of the Ziggurat data model and thus offers for the first time the technical basis for evaluating and verifying these design goals.

### Technical Implementation

All benchmarks will be implemented in Rust using [Criterion.rs](https://github.com/bheisler/criterion.rs) as a benchmark runner. Criterion should offer both a convenient API and a technical approach that ensures statistically sound results.

For implementing CWB3 benchmarks in Rust, a Rust wrapper around the old storage back end (libCL) is used. This should pose no significant overhead since Rust is able to directly interface with C libraries via its FFI system. This way of interfacing with existing C code is common practice in the Rust ecosystem.

All benchmarks will be run on a realistic test corpus that has been encoded for both CWB3 and Ziggurat and should be between 1 and 2 gigatokens in size. The corpus should contain different text genres in multiple languages and feature a realistic set of token level annotations (token, pos, lemma, universal dependencies [*if possible*]), segmentation (text, chunk/paragraph, sentence) and text metadata.
Additionally, a second, bigger version of the corpus should be used to test Ziggurat's scalability. This version should optimally contain 10+ gigatokens.

Data should be gathered on different machines running different operating systems and architectures. The current list of proposed targets is: x86-64 Laptop, x86-64 Workstation, x86-64 Server, Apple Silicon Laptop

The main goal of the following benchmarks should be to gather metrics on basic access patterns first and foremost. The performance of more complex use cases (like corpus queries) should in theory be a combination of several more simple access patterns. The proposed benchmarks include a number of combined patterns to verify the plausibility of this approach. 

## Benchmarks

### Preliminary Interop Tests

#### Wrapper Overhead

Implement some basic access patterns (like random access, TODO) for the libCL in C and Rust to rule out FFI overhead.

#### PCRE2 vs. rust-regex

Use/extend [this prior work](https://rust-leipzig.github.io/regex/2017/03/28/comparison-of-regex-engines/) to test the current performance of PCRE2 (used in CWB3) vs. the Rust regex crate (used in Etemenanki). Test with realistic regular expressions, used in linguistic use cases.

(The [result matrix in the linked GitHub repo](https://github.com/rust-leipzig/regex-performance#results) already outlines that rust-regex should be vastly more performant than PCRE2)


### Rust Performance Tests

#### rust-regex vs. std::str Patterns

Verify whether or not there is any merit in using the [simpler Pattern search capabilities](https://doc.rust-lang.org/std/str/pattern/trait.Pattern.html) of the Rust standard library as an optimization path for simple pre-, postfix and containment searches.


### Ziggurat Performance Tests

#### Sequential String Search vs. Lexicon Lookup

A regex is given and all of its (first n?) occurrences shall be found in an IndexedStringVariable. Compare the following strategies:

- Sequentially scanning the whole variable while regex-matching each token 
- Scanning the variable's lexicon for all matching strings and then using the variable's ReverseIndex component for actual result lookup

#### PointerVariable Decoding

Raw speed of decoding a pointer varibale in locally random patterns (random startpos and several accesses in a window +/- 20 tokens).

#### SegmentationLayer Lookup

Performance of determining the (start-position, end-position) tuple for a given corpus-position in several access patterns (sequential, random sequential windows, fully random).

#### Join Performance

Combined benchmark of the following lookup pattern: For a given cpos, determine its containing segment in a SegmentationLayer (segpos), then decode the segments start- and end-position. (cpos -> segpos -> cpos) 

#### Accessing XML-like Data

There are two strategies for encoding XML-like documents in Ziggurat. Spans in the input data can either be represented as separate SegmentationLayers for each span type (i.e. like s-attributes in CWB3) or collected in a singular TreeLayer. The performance impact of using a singlular TreeLayer is unknown at this point.


### libCL vs. Etemenanki comparisons

#### Sequential Layer Decode

Decoding a complete IndexedStringVariable from start to finish. This should give a good insight in the raw decoding performance of Ziggurat vs libCL.

#### Random Layer Decode

Decoding a complete IndexedStringVariable in random order. This should be able to reveal differences in block decoding and caching implementations.

#### Windowed Sequential Layer Decode

Decoding a complete IndexedStringVariable in randomly jumping windows of 20-50 tokens, sequential access within windows.

#### Narrowing Alternating Window Decode

Decoding a complete IndexedStringVariable in randomly jumping windows of 20-50 tokens, alternating access within windows (index jumps between start and end of window until reaching the middle).

#### Sequential, Head Locally Random Decode

Decoding a complete IndexedStringVariable from start to finish with occasional jumps in a window +/- 10 around the current position.

This could be extended in form of a realistic "pointer chasing" decode. With sufficiently annotated test data, a sequential search could be used to first find the "deepest" universal dependencies relation and then chase head pointers to the sentence root.
