package main

/*
#cgo LDFLAGS: -L${SRCDIR}/../rust/target/release -lsearch_ffi
#include <stdlib.h>

typedef void* SearchIndex;

SearchIndex search_index_open(const char* path);
void search_index_close(SearchIndex idx);
char* search_index_search(SearchIndex idx, const char* query, unsigned int limit, unsigned int offset);
char* search_index_stats(SearchIndex idx);
void search_string_free(char* s);
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"unsafe"
)

// Index wraps the Rust SearchIndex handle.
type Index struct {
	handle C.SearchIndex
}

// SearchResults from the Rust library.
type SearchResults struct {
	TotalHits int     `json:"total_hits"`
	TookMs    float64 `json:"took_ms"`
	Hits      []Hit   `json:"hits"`
	Error     string  `json:"error,omitempty"`
}

// Hit is a single search result.
type Hit struct {
	DocID uint32                 `json:"doc_id"`
	Score float32                `json:"score"`
	Doc   map[string]interface{} `json:"doc"`
}

// IndexStats from the Rust library.
type IndexStats struct {
	TotalDocs      uint64 `json:"total_docs"`
	TotalSegments  int    `json:"total_segments"`
	TotalTerms     uint64 `json:"total_terms"`
	IndexSizeBytes uint64 `json:"index_size_bytes"`
}

// OpenIndex opens an index directory.
func OpenIndex(path string) (*Index, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	handle := C.search_index_open(cPath)
	if handle == nil {
		return nil, fmt.Errorf("failed to open index at %s", path)
	}
	return &Index{handle: handle}, nil
}

// Close releases the index resources.
func (idx *Index) Close() {
	if idx.handle != nil {
		C.search_index_close(idx.handle)
		idx.handle = nil
	}
}

// Search executes a query and returns results.
func (idx *Index) Search(query string, limit, offset int) (*SearchResults, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	cResult := C.search_index_search(idx.handle, cQuery, C.uint(limit), C.uint(offset))
	if cResult == nil {
		return nil, fmt.Errorf("search returned null")
	}
	defer C.search_string_free(cResult)

	jsonStr := C.GoString(cResult)
	var results SearchResults
	if err := json.Unmarshal([]byte(jsonStr), &results); err != nil {
		return nil, fmt.Errorf("failed to parse results: %w", err)
	}
	if results.Error != "" {
		return nil, fmt.Errorf("%s", results.Error)
	}
	return &results, nil
}

// Stats returns index statistics.
func (idx *Index) Stats() (*IndexStats, error) {
	cResult := C.search_index_stats(idx.handle)
	if cResult == nil {
		return nil, fmt.Errorf("stats returned null")
	}
	defer C.search_string_free(cResult)

	jsonStr := C.GoString(cResult)
	var stats IndexStats
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		return nil, fmt.Errorf("failed to parse stats: %w", err)
	}
	return &stats, nil
}
