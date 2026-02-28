package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	indexDir := ""
	port := "8080"

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--index", "-i":
			i++
			indexDir = args[i]
		case "--port", "-p":
			i++
			port = args[i]
		case "--help", "-h":
			fmt.Println("Usage: search-server --index <index_dir> [--port 8080]")
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %s\n", args[i])
			os.Exit(1)
		}
	}

	if indexDir == "" {
		fmt.Fprintln(os.Stderr, "error: --index is required")
		os.Exit(1)
	}

	// Open the index
	idx, err := OpenIndex(indexDir)
	if err != nil {
		log.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	stats, err := idx.Stats()
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}
	log.Printf("Index opened: %d docs, %d segments, %d terms",
		stats.TotalDocs, stats.TotalSegments, stats.TotalTerms)

	// Initialize templates
	initTemplates()

	// Set up routes
	mux := http.NewServeMux()

	// Web interface
	mux.HandleFunc("/", searchHandler(idx))

	// API endpoints
	mux.HandleFunc("/api/search", apiSearchHandler(idx))
	mux.HandleFunc("/api/stats", apiStatsHandler(idx))
	mux.HandleFunc("/health", healthHandler)

	// Static files
	mux.Handle("/static/", http.FileServer(http.FS(staticFS)))

	log.Printf("Starting server on http://localhost:%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
