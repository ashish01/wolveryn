package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

var templates *template.Template

func initTemplates() {
	funcMap := template.FuncMap{
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"mul": func(a, b int) int { return a * b },
		"truncate": func(s string, n int) string {
			if len(s) <= n {
				return s
			}
			return s[:n] + "..."
		},
		"formatScore": func(f float32) string {
			return fmt.Sprintf("%.4f", f)
		},
		"formatDuration": func(ms float64) string {
			if ms < 1 {
				return fmt.Sprintf("%.2fms", ms)
			}
			return fmt.Sprintf("%.1fms", ms)
		},
		"formatBytes": func(b uint64) string {
			if b < 1024 {
				return fmt.Sprintf("%d B", b)
			} else if b < 1024*1024 {
				return fmt.Sprintf("%.1f KB", float64(b)/1024)
			} else if b < 1024*1024*1024 {
				return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
			}
			return fmt.Sprintf("%.2f GB", float64(b)/(1024*1024*1024))
		},
		"getField": func(doc map[string]interface{}, field string) string {
			if v, ok := doc[field]; ok {
				if s, ok := v.(string); ok {
					return s
				}
			}
			return ""
		},
		"snippet": func(doc map[string]interface{}) string {
			for _, field := range []string{"body", "text", "content", "description"} {
				if v, ok := doc[field]; ok {
					if s, ok := v.(string); ok {
						if len(s) > 200 {
							return s[:200] + "..."
						}
						return s
					}
				}
			}
			return ""
		},
		"title": func(doc map[string]interface{}) string {
			for _, field := range []string{"title", "name", "subject"} {
				if v, ok := doc[field]; ok {
					if s, ok := v.(string); ok {
						return s
					}
				}
			}
			return "(untitled)"
		},
	}

	templates = template.Must(
		template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html"),
	)
}

type SearchPageData struct {
	Query      string
	Results    *SearchResults
	Page       int
	Limit      int
	TotalPages int
	HasPrev    bool
	HasNext    bool
	PrevPage   int
	NextPage   int
	Stats      *IndexStats
}

func searchHandler(idx *Index) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		limitStr := r.URL.Query().Get("limit")
		pageStr := r.URL.Query().Get("page")

		limit := 10
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
			limit = l
		}

		page := 1
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}

		if query == "" {
			// Show search page
			stats, _ := idx.Stats()
			data := SearchPageData{Stats: stats}
			templates.ExecuteTemplate(w, "index.html", data)
			return
		}

		offset := (page - 1) * limit
		results, err := idx.Search(query, limit, offset)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		totalPages := int(math.Ceil(float64(results.TotalHits) / float64(limit)))
		stats, _ := idx.Stats()

		data := SearchPageData{
			Query:      query,
			Results:    results,
			Page:       page,
			Limit:      limit,
			TotalPages: totalPages,
			HasPrev:    page > 1,
			HasNext:    page < totalPages,
			PrevPage:   page - 1,
			NextPage:   page + 1,
			Stats:      stats,
		}

		templates.ExecuteTemplate(w, "results.html", data)
	}
}

func apiSearchHandler(idx *Index) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			http.Error(w, `{"error":"query parameter 'q' is required"}`, http.StatusBadRequest)
			return
		}

		limit := 10
		if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 {
			limit = l
		}

		offset := 0
		if o, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && o >= 0 {
			offset = o
		}

		results, err := idx.Search(query, limit, offset)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

func apiStatsHandler(idx *Index) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := idx.Stats()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}
