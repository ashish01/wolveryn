# Wolveryn

**Wolveryn** is a high-performance full-text search engine built from scratch, capable of handling 10M+ documents on a single node.

## Architecture

- **Rust core** — Inverted index, BM25 scoring, Block-Max WAND top-K retrieval, segment-based storage with memory-mapped I/O
- **Go service** — HTTP API + web search interface, calls Rust via C FFI/CGO

## What Was Built

Wolveryn is a from-scratch single-node search engine with 20 components across Rust + Go.

### Rust Core (102 unit tests)

| Component | File | What it does |
|---|---|---|
| Tokenizer | `analyzer.rs` | Unicode-aware, lowercase, zero deps |
| VByte codec | `vbyte.rs` | Variable-byte encoding with delta compression |
| BM25 scorer | `scorer.rs` | Scoring + field norm quantization (256 buckets) |
| Segment format | `segment.rs` | Binary format types for 7 segment files |
| Index writer | `index_writer.rs` | In-memory accumulation → 128-doc block flush |
| Term dictionary | `term_dict.rs` | Sorted binary search on mmap'd data |
| Postings reader | `postings.rs` | Block-aware `TermPostingIterator` with `shallow_advance()` |
| Boolean iterators | `iterator.rs` | AND/OR/NOT composing `PostingIterator` trait |
| Block-Max WAND | `bmw.rs` | Dynamic top-K pruning, exact results |
| Query parser | `query_parser.rs` | Recursive descent: `(a OR b) AND c NOT d` |
| Searcher | `searcher.rs` | End-to-end orchestrator, multi-segment |
| Merger | `merger.rs` | N segments → 1, remaps doc IDs |
| Indexer CLI | `indexer/main.rs` | JSONL → index with progress |
| Search CLI | `search-cli/main.rs` | Query from terminal |
| FFI library | `ffi/lib.rs` | C API (`cdylib`) with panic catching |

### Go Service (zero external deps)

| Component | What it does |
|---|---|
| CGO bindings | Calls `libsearch_ffi.dylib` |
| REST API | `/api/search`, `/api/stats`, `/health` |
| Web UI | Google-style search page + paginated results |

### Performance (10K docs)

- Indexing: 43K docs/sec
- Search: 0.04-0.52ms per query
- Dependencies: 3 Rust crates (`serde`, `serde_json`, `memmap2`), 0 Go deps

### Key Design Decisions

- **Iterator-based query evaluation** — `PostingIterator` trait with `next_doc()`, `advance()`, `shallow_advance()`. Boolean operators (AND/OR/NOT) compose iterators.
- **Block-Max WAND** — State-of-the-art dynamic pruning skips 85-95% of postings without any result quality loss.
- **Variable-byte compressed postings** — Delta-encoded doc IDs in 128-doc blocks, ~2-3 bytes per posting.
- **Minimal dependencies** — Rust: only `serde_json` + `memmap2`. Go: stdlib only. Everything else hand-written.

## Quick Start

### Build

```bash
make build
```

### Index Documents

```bash
# JSONL format: one JSON object per line with "title" and "body" fields
./rust/target/release/indexer index \
  --input data.jsonl \
  --output my_index \
  --fields title,body
```

### Search (CLI)

```bash
./rust/target/release/search-cli --index my_index --query "search engine"
```

### Search (Web)

```bash
# Set library path and start server
DYLD_LIBRARY_PATH=rust/target/release ./bin/search-server --index my_index --port 8080

# Open http://localhost:8080
```

### API

```bash
# Search
curl "http://localhost:8080/api/search?q=rust+programming&limit=10"

# Stats
curl http://localhost:8080/api/stats

# Health
curl http://localhost:8080/health
```

## Query Syntax

| Query | Meaning |
|-------|---------|
| `rust programming` | AND (implicit) — both terms required |
| `rust AND programming` | AND (explicit) |
| `rust OR python` | OR — either term |
| `NOT java` | NOT — exclude term |
| `(rust OR python) AND web` | Grouping with parentheses |

## Benchmarks

```bash
# Quick test (10K docs)
make bench

# Larger test (100K docs)
make bench-large

# Generate custom dataset
python3 scripts/bench_gen.py --docs 10000000 --output data.jsonl
```

## Project Structure

```
search_engine2/
├── rust/                       # Rust workspace
│   └── crates/
│       ├── core/               # Search engine library
│       │   └── src/
│       │       ├── analyzer.rs     # Unicode tokenizer
│       │       ├── vbyte.rs        # Variable-byte codec
│       │       ├── scorer.rs       # BM25 + field norm quantization
│       │       ├── segment.rs      # Segment format types
│       │       ├── index_writer.rs # In-memory accumulation + disk flush
│       │       ├── term_dict.rs    # Sorted binary search term dictionary
│       │       ├── postings.rs     # Block-aware postings reader
│       │       ├── iterator.rs     # AND/OR/NOT boolean iterators
│       │       ├── bmw.rs          # Block-Max WAND top-K collector
│       │       ├── query_parser.rs # Recursive descent parser
│       │       ├── searcher.rs     # End-to-end search coordinator
│       │       └── merger.rs       # Segment merge
│       ├── indexer/            # CLI: JSONL → index
│       ├── search-cli/         # CLI: query from terminal
│       └── ffi/                # C FFI shared library
├── go/                         # Go HTTP server + web UI
│   ├── main.go                 # Server entry point
│   ├── search.go               # CGO bindings
│   ├── handlers.go             # HTTP + web handlers
│   ├── templates/              # HTML templates
│   └── static/                 # CSS
├── scripts/
│   ├── build.sh                # Build everything
│   ├── bench_gen.py            # Generate synthetic data
│   └── bench_run.sh            # Run benchmarks
└── Makefile
```

## How It Works

### Indexing

1. Stream JSONL documents line by line
2. Tokenize text fields (lowercase, split on non-alphanumeric)
3. Accumulate in-memory inverted index (term → postings list)
4. At 500K docs: flush to immutable disk segment
5. Each segment: sorted term dictionary, block-compressed postings, stored documents

### Searching

1. Parse query into AST (recursive descent)
2. For each segment, build iterator tree from AST
3. **Single term / OR queries**: Block-Max WAND finds top-K without scoring all candidates
4. **AND queries**: Rarest term drives iteration, common terms probed via `advance()`
5. Merge results across segments, load stored documents

### Block-Max WAND

Postings are stored in 128-doc blocks. Each block has a precomputed upper-bound BM25 score. During top-K retrieval:

- The algorithm maintains a threshold θ (score of K-th best result)
- For each candidate, it checks if the sum of block-max scores can exceed θ
- If not, entire blocks are skipped via `shallow_advance()` (reads only 10-byte headers)
- Result quality is exact — BMW only skips provably irrelevant documents
