use search_core::index_writer::IndexWriter;
use search_core::merger;
use std::fs;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    match args[1].as_str() {
        "index" => cmd_index(&args[2..]),
        "merge" => cmd_merge(&args[2..]),
        _ => {
            eprintln!("unknown command: {}", args[1]);
            print_usage();
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  indexer index --input <file.jsonl> --output <index_dir> [--fields title,body] [--batch-size 500000]");
    eprintln!("  indexer merge --index <index_dir>");
}

fn cmd_index(args: &[String]) {
    let mut input: Option<String> = None;
    let mut output: Option<String> = None;
    let mut fields: Vec<String> = vec!["title".into(), "body".into()];
    let mut batch_size: usize = 500_000;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--input" | "-i" => {
                i += 1;
                input = Some(args[i].clone());
            }
            "--output" | "-o" => {
                i += 1;
                output = Some(args[i].clone());
            }
            "--fields" | "-f" => {
                i += 1;
                fields = args[i].split(',').map(|s| s.trim().to_string()).collect();
            }
            "--batch-size" | "-b" => {
                i += 1;
                batch_size = args[i].parse().expect("invalid batch size");
            }
            _ => {
                eprintln!("unknown flag: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let input = input.expect("--input is required");
    let output = output.expect("--output is required");

    let output_path = PathBuf::from(&output);
    fs::create_dir_all(&output_path).expect("failed to create output directory");

    eprintln!("Indexing {} → {}", input, output);
    eprintln!("Fields: {:?}, batch size: {}", fields, batch_size);

    let start = Instant::now();
    let mut total_docs: u64 = 0;
    let mut segment_count: u32 = 0;
    let mut malformed: u64 = 0;

    let reader: Box<dyn BufRead> = if input == "-" {
        Box::new(BufReader::new(io::stdin()))
    } else {
        let file = fs::File::open(&input).expect("failed to open input file");
        Box::new(BufReader::with_capacity(1 << 20, file))
    };

    let mut writer = IndexWriter::new(fields.clone(), 0);

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("warning: read error: {}", e);
                malformed += 1;
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let doc: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                if malformed < 10 {
                    eprintln!("warning: malformed JSON (line {}): {}", total_docs + malformed + 1, e);
                }
                malformed += 1;
                continue;
            }
        };

        writer.add_document(&doc);
        total_docs += 1;

        if writer.doc_count() as usize >= batch_size {
            let seg_dir = output_path.join(format!("segment_{}", segment_count));
            let meta = writer.flush_segment(&seg_dir).expect("failed to flush segment");
            eprintln!(
                "  Flushed segment_{}: {} docs, {:.1}s elapsed",
                segment_count,
                meta.doc_count,
                start.elapsed().as_secs_f64()
            );
            segment_count += 1;
        }
    }

    // Flush remaining docs
    if writer.doc_count() > 0 {
        let seg_dir = output_path.join(format!("segment_{}", segment_count));
        let meta = writer.flush_segment(&seg_dir).expect("failed to flush segment");
        eprintln!(
            "  Flushed segment_{}: {} docs, {:.1}s elapsed",
            segment_count,
            meta.doc_count,
            start.elapsed().as_secs_f64()
        );
        segment_count += 1;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let docs_per_sec = if elapsed > 0.0 {
        total_docs as f64 / elapsed
    } else {
        0.0
    };

    eprintln!();
    eprintln!("Done!");
    eprintln!("  Documents indexed: {}", total_docs);
    eprintln!("  Malformed lines:   {}", malformed);
    eprintln!("  Segments created:  {}", segment_count);
    eprintln!("  Time:              {:.2}s", elapsed);
    eprintln!("  Throughput:        {:.0} docs/sec", docs_per_sec);
}

fn cmd_merge(args: &[String]) {
    let mut index_dir: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--index" | "-i" => {
                i += 1;
                index_dir = Some(args[i].clone());
            }
            _ => {
                eprintln!("unknown flag: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let index_dir = PathBuf::from(index_dir.expect("--index is required"));

    // Find all segment directories
    let mut seg_dirs: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(&index_dir).expect("failed to read index dir") {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if entry.path().is_dir() && name.starts_with("segment_") {
            seg_dirs.push(entry.path());
        }
    }
    seg_dirs.sort();

    if seg_dirs.len() <= 1 {
        eprintln!("Only {} segment(s), nothing to merge.", seg_dirs.len());
        return;
    }

    eprintln!("Merging {} segments...", seg_dirs.len());
    let start = Instant::now();

    let merged_dir = index_dir.join("merged_temp");
    let seg_refs: Vec<&Path> = seg_dirs.iter().map(|p| p.as_path()).collect();
    let meta = merger::merge_segments(&seg_refs, &merged_dir).expect("merge failed");

    // Remove old segments
    for dir in &seg_dirs {
        fs::remove_dir_all(dir).expect("failed to remove old segment");
    }

    // Rename merged to segment_0
    fs::rename(&merged_dir, index_dir.join("segment_0")).expect("failed to rename merged segment");

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!("Done! Merged into 1 segment: {} docs, {} terms, {:.2}s",
        meta.doc_count, meta.term_count, elapsed);
}
