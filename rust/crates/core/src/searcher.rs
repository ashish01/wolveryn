/// Searcher: ties together all components for end-to-end search.
///
/// Opens an index directory, parses queries, builds iterator trees,
/// runs BMW collection, and returns ranked results with stored documents.

use crate::bmw::{self, ScoredDoc, TermScorer};
use crate::postings::{PostingIterator, TermPostingIterator, NO_MORE_DOCS};
use crate::query_parser::{self, Query};
use crate::scorer;
use crate::segment::{self, DocIndexEntry, PostingsIndexEntry, SegmentMeta};
use crate::term_dict::TermDict;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// DF ceiling: terms appearing in >50% of docs are pruned from queries.
/// Only applies when total docs exceeds MIN_DOCS_FOR_DF_CEILING.
const DF_CEILING: f32 = 0.5;
const MIN_DOCS_FOR_DF_CEILING: u32 = 1000;

/// A search hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hit {
    pub doc_id: u32,
    pub score: f32,
    pub doc: serde_json::Value,
}

/// Search results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResults {
    pub total_hits: usize,
    pub took_ms: f64,
    pub hits: Vec<Hit>,
}

/// Index statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub total_docs: u64,
    pub total_segments: usize,
    pub total_terms: u64,
    pub index_size_bytes: u64,
}

/// An opened segment with all its mmap'd files.
struct OpenSegment {
    meta: SegmentMeta,
    term_dict: TermDict,
    postings_mmap: Mmap,
    postings_index_mmap: Mmap,
    fieldnorms: Vec<u32>,
    docs_mmap: Mmap,
    docs_index_mmap: Mmap,
    #[allow(dead_code)]
    dir: PathBuf,
}

impl OpenSegment {
    fn open(segment_dir: &Path) -> std::io::Result<Self> {
        let files = segment::segment_files(segment_dir);

        let meta_str = fs::read_to_string(&files.meta)?;
        let meta: SegmentMeta = serde_json::from_str(&meta_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let term_dict = TermDict::open(&files.terms)?;

        let postings_file = fs::File::open(&files.postings)?;
        let postings_mmap = unsafe { Mmap::map(&postings_file)? };

        let pi_file = fs::File::open(&files.postings_index)?;
        let postings_index_mmap = unsafe { Mmap::map(&pi_file)? };

        let norm_bytes = fs::read(&files.fieldnorms)?;
        let norm_table = scorer::build_field_norm_table();
        let fieldnorms: Vec<u32> = norm_bytes.iter().map(|&b| norm_table[b as usize]).collect();

        let docs_file = fs::File::open(&files.docs)?;
        let docs_mmap = unsafe { Mmap::map(&docs_file)? };

        let di_file = fs::File::open(&files.docs_index)?;
        let docs_index_mmap = unsafe { Mmap::map(&di_file)? };

        Ok(Self {
            meta,
            term_dict,
            postings_mmap,
            postings_index_mmap,
            fieldnorms,
            docs_mmap,
            docs_index_mmap,
            dir: segment_dir.to_path_buf(),
        })
    }

    fn get_postings_entry(&self, term_id: u32) -> PostingsIndexEntry {
        let offset = term_id as usize * PostingsIndexEntry::SIZE;
        PostingsIndexEntry::from_bytes(&self.postings_index_mmap[offset..])
    }

    fn load_document(&self, local_doc_id: u32) -> serde_json::Value {
        let offset = local_doc_id as usize * DocIndexEntry::SIZE;
        if offset + DocIndexEntry::SIZE > self.docs_index_mmap.len() {
            return serde_json::Value::Null;
        }
        let entry = DocIndexEntry::from_bytes(&self.docs_index_mmap[offset..]);
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end > self.docs_mmap.len() {
            return serde_json::Value::Null;
        }
        serde_json::from_slice(&self.docs_mmap[start..end]).unwrap_or(serde_json::Value::Null)
    }
}

/// The main searcher. Holds opened segments and provides search functionality.
pub struct Searcher {
    segments: Vec<OpenSegment>,
    index_dir: PathBuf,
}

impl Searcher {
    /// Open an index directory containing segment subdirectories.
    pub fn open(index_dir: &Path) -> std::io::Result<Self> {
        let mut segments = Vec::new();

        if !index_dir.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("index directory not found: {}", index_dir.display()),
            ));
        }

        // Find segment directories (segment_0, segment_1, ...)
        let mut entries: Vec<_> = fs::read_dir(index_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir() && e.file_name().to_str().map_or(false, |n| n.starts_with("segment_")))
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            match OpenSegment::open(&entry.path()) {
                Ok(seg) => segments.push(seg),
                Err(e) => eprintln!("warning: failed to open segment {:?}: {}", entry.path(), e),
            }
        }

        Ok(Self {
            segments,
            index_dir: index_dir.to_path_buf(),
        })
    }

    /// Execute a search query.
    pub fn search(&self, query_str: &str, limit: usize, offset: usize) -> Result<SearchResults, String> {
        let start = Instant::now();

        // Parse query
        let query = query_parser::parse(query_str)?;

        // Collect results across all segments
        let mut all_results: Vec<ScoredDoc> = Vec::new();
        let total_docs: u32 = self.segments.iter().map(|s| s.meta.doc_count).sum();

        for seg in &self.segments {
            let results = self.search_segment(seg, &query, limit + offset, total_docs)?;
            all_results.extend(results);
        }

        // Merge results from all segments by score
        all_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        let total_hits = all_results.len();

        // Apply offset and limit
        let hits: Vec<Hit> = all_results
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|sd| {
                let doc = self.load_document(sd.doc_id);
                Hit {
                    doc_id: sd.doc_id,
                    score: sd.score,
                    doc,
                }
            })
            .collect();

        let took_ms = start.elapsed().as_secs_f64() * 1000.0;

        Ok(SearchResults {
            total_hits,
            took_ms,
            hits,
        })
    }

    /// Search a single segment.
    fn search_segment(
        &self,
        seg: &OpenSegment,
        query: &Query,
        top_k: usize,
        total_docs: u32,
    ) -> Result<Vec<ScoredDoc>, String> {
        // For simple term queries and OR queries, use BMW directly
        // For AND/NOT, build iterator tree and use exhaustive scoring
        match query {
            Query::Term(term) => {
                self.search_term_bmw(seg, term, top_k, total_docs)
            }
            Query::And(children) => {
                self.search_and(seg, children, top_k, total_docs)
            }
            Query::Or(children) => {
                self.search_or_bmw(seg, children, top_k, total_docs)
            }
            Query::Not(inner) => {
                // NOT alone doesn't make sense for scoring.
                // It should only appear inside AND.
                Err(format!("NOT query must be combined with AND: NOT {:?}", inner))
            }
        }
    }

    /// Search for a single term using BMW.
    fn search_term_bmw(
        &self,
        seg: &OpenSegment,
        term: &str,
        top_k: usize,
        total_docs: u32,
    ) -> Result<Vec<ScoredDoc>, String> {
        let term_id = match seg.term_dict.lookup(term) {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let entry = seg.get_postings_entry(term_id);

        // DF ceiling check
        if exceeds_df_ceiling(entry.doc_freq, total_docs) {
            return Ok(Vec::new());
        }

        let iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
        let idf = scorer::idf(entry.doc_freq, total_docs);

        let mut scorers = vec![TermScorer {
            iter: Box::new(iter),
            idf,
            avgdl: seg.meta.avgdl,
            cur_doc: NO_MORE_DOCS,
        }];

        Ok(bmw::block_max_wand(
            &mut scorers,
            &seg.fieldnorms,
            top_k,
            seg.meta.min_doc_id,
        ))
    }

    /// Search OR query using BMW with multiple term scorers.
    fn search_or_bmw(
        &self,
        seg: &OpenSegment,
        children: &[Query],
        top_k: usize,
        total_docs: u32,
    ) -> Result<Vec<ScoredDoc>, String> {
        let mut scorers: Vec<TermScorer> = Vec::new();

        for child in children {
            match child {
                Query::Term(term) => {
                    if let Some(term_id) = seg.term_dict.lookup(term) {
                        let entry = seg.get_postings_entry(term_id);
                        if !exceeds_df_ceiling(entry.doc_freq, total_docs) {
                            let iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
                            let idf = scorer::idf(entry.doc_freq, total_docs);
                            scorers.push(TermScorer {
                                iter: Box::new(iter),
                                idf,
                                avgdl: seg.meta.avgdl,
                                cur_doc: NO_MORE_DOCS,
                            });
                        }
                    }
                }
                _ => {
                    // Complex sub-queries inside OR: fall back to exhaustive
                    return self.search_or_exhaustive(seg, children, top_k, total_docs);
                }
            }
        }

        if scorers.is_empty() {
            return Ok(Vec::new());
        }

        Ok(bmw::block_max_wand(
            &mut scorers,
            &seg.fieldnorms,
            top_k,
            seg.meta.min_doc_id,
        ))
    }

    /// Fallback: OR with complex sub-queries.
    fn search_or_exhaustive(
        &self,
        seg: &OpenSegment,
        children: &[Query],
        top_k: usize,
        total_docs: u32,
    ) -> Result<Vec<ScoredDoc>, String> {
        let mut all: Vec<ScoredDoc> = Vec::new();
        for child in children {
            let results = self.search_segment(seg, child, top_k, total_docs)?;
            all.extend(results);
        }
        // Deduplicate by doc_id, keeping highest score
        all.sort_by_key(|r| r.doc_id);
        all.dedup_by(|a, b| {
            if a.doc_id == b.doc_id {
                b.score = b.score.max(a.score);
                true
            } else {
                false
            }
        });
        all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(top_k);
        Ok(all)
    }

    /// Search AND query using BMW with per-term scorers.
    /// For AND, we use BMW but only score docs that appear in ALL terms.
    fn search_and(
        &self,
        seg: &OpenSegment,
        children: &[Query],
        top_k: usize,
        total_docs: u32,
    ) -> Result<Vec<ScoredDoc>, String> {
        // Flatten AND children: extract terms and NOT terms
        let mut term_names: Vec<String> = Vec::new();
        let mut not_terms: Vec<String> = Vec::new();

        for child in children {
            match child {
                Query::Term(t) => term_names.push(t.clone()),
                Query::Not(inner) => {
                    if let Query::Term(t) = inner.as_ref() {
                        not_terms.push(t.clone());
                    }
                }
                _ => {
                    // Complex sub-expression inside AND — fall back to building
                    // full iterator tree. For now, just collect terms from sub-expr.
                    collect_terms(child, &mut term_names);
                }
            }
        }

        // Build term iterators for AND intersection
        let mut term_iters: Vec<(TermPostingIterator<'_>, f32)> = Vec::new();
        for term in &term_names {
            if let Some(term_id) = seg.term_dict.lookup(term) {
                let entry = seg.get_postings_entry(term_id);
                if exceeds_df_ceiling(entry.doc_freq, total_docs) {
                    continue; // Skip overly common terms
                }
                let iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
                let idf = scorer::idf(entry.doc_freq, total_docs);
                term_iters.push((iter, idf));
            } else {
                // Term not in segment → AND produces no results
                return Ok(Vec::new());
            }
        }

        if term_iters.is_empty() {
            return Ok(Vec::new());
        }

        // Build NOT exclusion set
        let mut not_doc_ids: Vec<u32> = Vec::new();
        for term in &not_terms {
            if let Some(term_id) = seg.term_dict.lookup(term) {
                let entry = seg.get_postings_entry(term_id);
                let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
                while let Some(doc_id) = iter.next_doc() {
                    not_doc_ids.push(doc_id);
                }
            }
        }
        not_doc_ids.sort();
        not_doc_ids.dedup();

        // Sort by df ascending (rarest first drives AND)
        term_iters.sort_by_key(|(iter, _)| iter.doc_freq());

        // AND intersection with scoring
        let mut results: Vec<ScoredDoc> = Vec::new();

        // Use the rarest term as the driver
        let (mut lead, lead_idf) = term_iters.remove(0);
        let mut others: Vec<(TermPostingIterator<'_>, f32)> = term_iters;

        while let Some(candidate) = lead.next_doc() {
            // Check NOT exclusion
            if not_doc_ids.binary_search(&candidate).is_ok() {
                continue;
            }

            // Verify all other terms have this doc
            let mut all_match = true;
            let mut total_score = {
                let local_id = (candidate - seg.meta.min_doc_id) as usize;
                let dl = if local_id < seg.fieldnorms.len() { seg.fieldnorms[local_id] } else { 1 };
                scorer::bm25_term_score(lead.term_freq(), dl, seg.meta.avgdl, lead_idf)
            };

            for (iter, idf) in &mut others {
                match iter.advance(candidate) {
                    Some(doc) if doc == candidate => {
                        let local_id = (candidate - seg.meta.min_doc_id) as usize;
                        let dl = if local_id < seg.fieldnorms.len() { seg.fieldnorms[local_id] } else { 1 };
                        total_score += scorer::bm25_term_score(iter.term_freq(), dl, seg.meta.avgdl, *idf);
                    }
                    _ => {
                        all_match = false;
                        break;
                    }
                }
            }

            if all_match {
                results.push(ScoredDoc {
                    doc_id: candidate,
                    score: total_score,
                });
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(top_k);
        Ok(results)
    }

    /// Reload segments (e.g., after indexing or merging).
    pub fn reload(&mut self) -> std::io::Result<()> {
        let new = Searcher::open(&self.index_dir)?;
        self.segments = new.segments;
        Ok(())
    }

    /// Load a stored document by global doc_id.
    fn load_document(&self, doc_id: u32) -> serde_json::Value {
        for seg in &self.segments {
            if doc_id >= seg.meta.min_doc_id && doc_id <= seg.meta.max_doc_id {
                let local_id = doc_id - seg.meta.min_doc_id;
                return seg.load_document(local_id);
            }
        }
        serde_json::Value::Null
    }

    /// Get index statistics.
    pub fn stats(&self) -> IndexStats {
        let total_docs: u64 = self.segments.iter().map(|s| s.meta.doc_count as u64).sum();
        let total_terms: u64 = self.segments.iter().map(|s| s.meta.term_count as u64).sum();

        // Calculate index size
        let index_size = self.calculate_index_size();

        IndexStats {
            total_docs,
            total_segments: self.segments.len(),
            total_terms,
            index_size_bytes: index_size,
        }
    }

    fn calculate_index_size(&self) -> u64 {
        let mut total: u64 = 0;
        if let Ok(entries) = fs::read_dir(&self.index_dir) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    if let Ok(sub) = fs::read_dir(entry.path()) {
                        for f in sub.flatten() {
                            total += f.metadata().map(|m| m.len()).unwrap_or(0);
                        }
                    }
                }
            }
        }
        total
    }
}

/// Check if a term should be pruned by DF ceiling.
fn exceeds_df_ceiling(doc_freq: u32, total_docs: u32) -> bool {
    total_docs >= MIN_DOCS_FOR_DF_CEILING
        && (doc_freq as f32 / total_docs as f32) > DF_CEILING
}

/// Recursively collect term strings from a query.
fn collect_terms(query: &Query, terms: &mut Vec<String>) {
    match query {
        Query::Term(t) => terms.push(t.clone()),
        Query::And(children) | Query::Or(children) => {
            for c in children {
                collect_terms(c, terms);
            }
        }
        Query::Not(_) => {} // skip NOT terms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_writer::IndexWriter;
    use serde_json::json;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "search_test_searcher_{}_{}",
            std::process::id(),
            name
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn build_index(name: &str, docs: Vec<serde_json::Value>) -> PathBuf {
        let dir = test_dir(name);
        let seg_dir = dir.join("segment_0");

        let mut writer = IndexWriter::new(vec!["title".into(), "body".into()], 0);
        for doc in &docs {
            writer.add_document(doc);
        }
        writer.flush_segment(&seg_dir).unwrap();
        dir
    }

    #[test]
    fn test_end_to_end_search() {
        let dir = build_index("e2e", vec![
            json!({"title": "Rust Programming", "body": "Rust is a systems programming language"}),
            json!({"title": "Python Guide", "body": "Python is great for scripting"}),
            json!({"title": "Rust Web", "body": "Build web applications with Rust"}),
        ]);

        let searcher = Searcher::open(&dir).unwrap();
        let results = searcher.search("rust", 10, 0).unwrap();

        assert_eq!(results.total_hits, 2);
        assert_eq!(results.hits.len(), 2);
        assert!(results.took_ms >= 0.0);

        // Both hits should have "rust" related docs
        for hit in &results.hits {
            assert!(hit.score > 0.0);
            assert!(!hit.doc.is_null());
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_or_query() {
        let dir = build_index("or_query", vec![
            json!({"title": "Rust", "body": "systems language"}),
            json!({"title": "Python", "body": "scripting language"}),
            json!({"title": "Go", "body": "compiled language"}),
        ]);

        let searcher = Searcher::open(&dir).unwrap();
        let results = searcher.search("rust OR python", 10, 0).unwrap();

        assert_eq!(results.total_hits, 2);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_and_query() {
        let dir = build_index("and_query", vec![
            json!({"title": "Rust Web", "body": "build web apps with rust"}),
            json!({"title": "Rust CLI", "body": "build cli tools with rust"}),
            json!({"title": "Python Web", "body": "build web apps with python"}),
        ]);

        let searcher = Searcher::open(&dir).unwrap();
        let results = searcher.search("rust web", 10, 0).unwrap();

        // "rust" AND "web" — only doc 0 has both
        assert!(results.total_hits >= 1);
        assert!(results.hits[0].doc_id == 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_no_results() {
        let dir = build_index("no_results", vec![
            json!({"title": "Hello", "body": "World"}),
        ]);

        let searcher = Searcher::open(&dir).unwrap();
        let results = searcher.search("nonexistent", 10, 0).unwrap();

        assert_eq!(results.total_hits, 0);
        assert!(results.hits.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_pagination() {
        let docs: Vec<_> = (0..20)
            .map(|i| json!({"title": format!("Doc {}", i), "body": "common term here"}))
            .collect();
        let dir = build_index("pagination", docs);

        let searcher = Searcher::open(&dir).unwrap();

        let page1 = searcher.search("common", 5, 0).unwrap();
        let page2 = searcher.search("common", 5, 5).unwrap();

        assert_eq!(page1.hits.len(), 5);
        assert_eq!(page2.hits.len(), 5);

        // Pages should have different doc_ids
        let ids1: Vec<u32> = page1.hits.iter().map(|h| h.doc_id).collect();
        let ids2: Vec<u32> = page2.hits.iter().map(|h| h.doc_id).collect();
        for id in &ids1 {
            assert!(!ids2.contains(id));
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_stats() {
        let dir = build_index("stats", vec![
            json!({"title": "A", "body": "hello world"}),
            json!({"title": "B", "body": "foo bar"}),
        ]);

        let searcher = Searcher::open(&dir).unwrap();
        let stats = searcher.stats();

        assert_eq!(stats.total_docs, 2);
        assert_eq!(stats.total_segments, 1);
        assert!(stats.total_terms > 0);
        assert!(stats.index_size_bytes > 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_multi_segment() {
        let dir = test_dir("multi_seg");

        // Write two segments
        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({"body": "rust programming"}));
        writer.add_document(&json!({"body": "rust web"}));
        writer.flush_segment(&dir.join("segment_0")).unwrap();

        writer.add_document(&json!({"body": "rust fast"}));
        writer.add_document(&json!({"body": "python slow"}));
        writer.flush_segment(&dir.join("segment_1")).unwrap();

        let searcher = Searcher::open(&dir).unwrap();
        let results = searcher.search("rust", 10, 0).unwrap();

        assert_eq!(results.total_hits, 3); // docs 0, 1, 2 have "rust"

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_stored_documents_returned() {
        let dir = build_index("stored_docs", vec![
            json!({"title": "Test Doc", "body": "search engine test"}),
        ]);

        let searcher = Searcher::open(&dir).unwrap();
        let results = searcher.search("search", 10, 0).unwrap();

        assert_eq!(results.hits.len(), 1);
        assert_eq!(results.hits[0].doc["title"], "Test Doc");
        assert_eq!(results.hits[0].doc["body"], "search engine test");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_bad_query() {
        let dir = build_index("bad_query", vec![
            json!({"title": "A", "body": "hello"}),
        ]);

        let searcher = Searcher::open(&dir).unwrap();
        assert!(searcher.search("", 10, 0).is_err());
        assert!(searcher.search("(unclosed", 10, 0).is_err());

        let _ = fs::remove_dir_all(&dir);
    }
}
