use search_core::searcher::Searcher;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut index_dir: Option<String> = None;
    let mut query: Option<String> = None;
    let mut limit: usize = 10;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--index" | "-i" => {
                i += 1;
                index_dir = Some(args[i].clone());
            }
            "--query" | "-q" => {
                i += 1;
                query = Some(args[i].clone());
            }
            "--limit" | "-l" => {
                i += 1;
                limit = args[i].parse().expect("invalid limit");
            }
            "--help" | "-h" => {
                print_usage();
                return;
            }
            _ => {
                // If no flag, treat as query
                if query.is_none() && !args[i].starts_with('-') {
                    query = Some(args[i].clone());
                } else {
                    eprintln!("unknown flag: {}", args[i]);
                    print_usage();
                    std::process::exit(1);
                }
            }
        }
        i += 1;
    }

    let index_dir = PathBuf::from(index_dir.expect("--index is required"));
    let query_str = query.expect("--query is required");

    // Open index
    let searcher = match Searcher::open(&index_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to open index at {}: {}", index_dir.display(), e);
            std::process::exit(1);
        }
    };

    // Print stats
    let stats = searcher.stats();
    eprintln!(
        "Index: {} docs, {} segments, {} terms",
        stats.total_docs, stats.total_segments, stats.total_terms
    );
    eprintln!();

    // Execute query
    match searcher.search(&query_str, limit, 0) {
        Ok(results) => {
            eprintln!(
                "Found {} results in {:.2}ms",
                results.total_hits, results.took_ms
            );
            eprintln!();

            for (rank, hit) in results.hits.iter().enumerate() {
                println!("{}. [score: {:.4}] doc_id={}", rank + 1, hit.score, hit.doc_id);

                // Try to show title
                if let Some(title) = hit.doc.get("title").and_then(|v| v.as_str()) {
                    println!("   Title: {}", title);
                }

                // Show snippet from body or first text field
                if let Some(body) = hit.doc.get("body").and_then(|v| v.as_str()) {
                    let snippet: String = body.chars().take(200).collect();
                    println!("   {}", snippet);
                } else if let Some(obj) = hit.doc.as_object() {
                    // Show first string field
                    for (key, val) in obj {
                        if key == "id" {
                            continue;
                        }
                        if let Some(s) = val.as_str() {
                            let snippet: String = s.chars().take(200).collect();
                            println!("   {}: {}", key, snippet);
                            break;
                        }
                    }
                }

                println!();
            }
        }
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("Usage: search-cli --index <index_dir> --query \"search terms\" [--limit 10]");
}
