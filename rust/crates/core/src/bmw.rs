/// Block-Max WAND (BMW) top-K collector.
///
/// Dynamically prunes documents that provably can't reach the top-K threshold.
/// Uses per-block upper-bound scores to skip entire blocks without decoding.
/// Result quality is exact — no approximation.

use crate::postings::{PostingIterator, NO_MORE_DOCS};
use crate::scorer;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

/// A scored search result.
#[derive(Debug, Clone)]
pub struct ScoredDoc {
    pub doc_id: u32,
    pub score: f32,
}

/// Min-heap entry (inverted ordering for BinaryHeap which is a max-heap).
#[derive(Debug, Clone)]
struct HeapEntry {
    doc_id: u32,
    score: f32,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering to make BinaryHeap act as min-heap
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
            .then(other.doc_id.cmp(&self.doc_id))
    }
}

/// A term iterator with precomputed scoring metadata for BMW.
pub struct TermScorer<'a> {
    pub iter: Box<dyn PostingIterator + 'a>,
    pub idf: f32,
    pub avgdl: f32,
    /// Current doc_id this scorer is positioned at.
    pub cur_doc: u32,
}

impl<'a> TermScorer<'a> {
    /// Compute BM25 score for the current document.
    pub fn score(&self, dl: u32) -> f32 {
        scorer::bm25_term_score(self.iter.term_freq(), dl, self.avgdl, self.idf)
    }

    /// Upper-bound score for the current block.
    pub fn max_score(&self) -> f32 {
        self.iter.max_block_score()
    }
}

/// Run Block-Max WAND to find the top-K documents.
///
/// - `scorers`: term scorers (one per query term), each wrapping a PostingIterator
/// - `fieldnorms`: decoded field lengths array (index = local_doc_id)
/// - `k`: number of results to return
/// - `base_doc_id`: offset to convert local doc_ids to global
///
/// Returns results sorted by score descending.
pub fn block_max_wand<'a>(
    scorers: &mut Vec<TermScorer<'a>>,
    fieldnorms: &[u32],
    k: usize,
    base_doc_id: u32,
) -> Vec<ScoredDoc> {
    if scorers.is_empty() || k == 0 {
        return Vec::new();
    }

    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k + 1);
    let mut threshold: f32 = 0.0;

    // Initialize all scorers to their first document
    for scorer in scorers.iter_mut() {
        scorer.cur_doc = scorer.iter.next_doc().unwrap_or(NO_MORE_DOCS);
    }

    loop {
        // Sort scorers by current doc_id
        scorers.sort_by_key(|s| s.cur_doc);

        // Remove exhausted scorers
        while !scorers.is_empty() && scorers.last().unwrap().cur_doc == NO_MORE_DOCS {
            scorers.pop();
        }
        if scorers.is_empty() {
            break;
        }

        // Find pivot: smallest index p where sum of max_block_scores[0..=p] >= threshold
        let pivot = find_pivot(scorers, threshold);
        if pivot.is_none() {
            break; // No combination of remaining terms can beat threshold
        }
        let pivot_idx = pivot.unwrap();
        let pivot_doc = scorers[pivot_idx].cur_doc;

        if pivot_doc == NO_MORE_DOCS {
            break;
        }

        // Check if all scorers [0..=pivot_idx] are at pivot_doc
        if scorers[0].cur_doc == pivot_doc {
            // All required scorers converge — score this document
            let local_id = (pivot_doc - base_doc_id) as usize;
            let dl = if local_id < fieldnorms.len() {
                fieldnorms[local_id]
            } else {
                1
            };

            let mut total_score: f32 = 0.0;
            for scorer in scorers.iter() {
                if scorer.cur_doc == pivot_doc {
                    total_score += scorer.score(dl);
                }
            }

            if total_score > threshold || heap.len() < k {
                heap.push(HeapEntry {
                    doc_id: pivot_doc,
                    score: total_score,
                });
                if heap.len() > k {
                    heap.pop(); // Remove minimum
                }
                if heap.len() == k {
                    threshold = heap.peek().unwrap().score;
                }
            }

            // Advance all scorers at pivot_doc
            for scorer in scorers.iter_mut() {
                if scorer.cur_doc == pivot_doc {
                    scorer.cur_doc = scorer.iter.next_doc().unwrap_or(NO_MORE_DOCS);
                }
            }
        } else {
            // Gap exists — some scorers are behind pivot_doc.
            // Try block-level pruning with shallow_advance.
            let can_skip = try_block_skip(scorers, pivot_idx, pivot_doc, threshold);

            if can_skip {
                // Block can't beat threshold — skip to end of block + 1
                let skip_to = scorers[0].iter.block_max_doc().saturating_add(1);
                for scorer in scorers.iter_mut() {
                    if scorer.cur_doc < skip_to && scorer.cur_doc != NO_MORE_DOCS {
                        scorer.cur_doc = scorer.iter.advance(skip_to).unwrap_or(NO_MORE_DOCS);
                    }
                }
            } else {
                // Can't skip — advance lagging scorers to pivot_doc
                for i in 0..pivot_idx {
                    if scorers[i].cur_doc < pivot_doc {
                        scorers[i].cur_doc = scorers[i]
                            .iter
                            .advance(pivot_doc)
                            .unwrap_or(NO_MORE_DOCS);
                    }
                }
            }
        }
    }

    // Extract results sorted by score descending
    let mut results: Vec<ScoredDoc> = heap
        .into_iter()
        .map(|e| ScoredDoc {
            doc_id: e.doc_id,
            score: e.score,
        })
        .collect();
    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
    results
}

/// Find pivot index: smallest p where sum of max_block_scores[0..=p] >= threshold.
fn find_pivot(scorers: &[TermScorer], threshold: f32) -> Option<usize> {
    let mut cumulative = 0.0f32;
    for (i, scorer) in scorers.iter().enumerate() {
        cumulative += scorer.max_score();
        if cumulative >= threshold {
            return Some(i);
        }
    }
    // If threshold is 0 (heap not yet full), pivot at last scorer
    if threshold == 0.0 && !scorers.is_empty() {
        return Some(scorers.len() - 1);
    }
    None
}

/// Try block-level skip: shallow_advance lagging scorers and check if
/// the block's combined max scores can beat the threshold.
fn try_block_skip(
    scorers: &mut [TermScorer],
    pivot_idx: usize,
    pivot_doc: u32,
    threshold: f32,
) -> bool {
    // Shallow advance lagging scorers to the block containing pivot_doc
    let mut block_max_sum: f32 = 0.0;
    for i in 0..=pivot_idx {
        if scorers[i].cur_doc < pivot_doc {
            scorers[i].iter.shallow_advance(pivot_doc);
        }
        block_max_sum += scorers[i].iter.max_block_score();
    }

    // If block-max sum can't beat threshold, we can skip this block
    block_max_sum < threshold
}

/// Simple exhaustive top-K collector (fallback for AND queries).
/// Pulls all docs from an iterator and scores them.
pub fn exhaustive_top_k<'a>(
    iter: &mut dyn PostingIterator,
    fieldnorms: &[u32],
    idf_val: f32,
    avgdl: f32,
    k: usize,
    base_doc_id: u32,
) -> Vec<ScoredDoc> {
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k + 1);

    while let Some(doc_id) = iter.next_doc() {
        let local_id = (doc_id - base_doc_id) as usize;
        let dl = if local_id < fieldnorms.len() {
            fieldnorms[local_id]
        } else {
            1
        };
        let score = scorer::bm25_term_score(iter.term_freq(), dl, avgdl, idf_val);

        if heap.len() < k || score > heap.peek().unwrap().score {
            heap.push(HeapEntry { doc_id, score });
            if heap.len() > k {
                heap.pop();
            }
        }
    }

    let mut results: Vec<ScoredDoc> = heap
        .into_iter()
        .map(|e| ScoredDoc {
            doc_id: e.doc_id,
            score: e.score,
        })
        .collect();
    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_writer::IndexWriter;
    use crate::postings::TermPostingIterator;
    use crate::segment::{self, PostingsIndexEntry};
    use crate::term_dict::TermDict;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("search_test_bmw_{}_{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    struct TestIndex {
        _dir: PathBuf,
        postings_mmap: memmap2::Mmap,
        index_data: Vec<u8>,
        term_dict: TermDict,
        meta: segment::SegmentMeta,
        fieldnorms: Vec<u32>,
    }

    fn create_index(name: &str, docs: Vec<serde_json::Value>) -> TestIndex {
        let dir = test_dir(name);
        let seg_dir = dir.join("segment_0");

        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        for doc in &docs {
            writer.add_document(doc);
        }
        let meta = writer.flush_segment(&seg_dir).unwrap();

        let files = segment::segment_files(&seg_dir);
        let postings_file = fs::File::open(&files.postings).unwrap();
        let postings_mmap = unsafe { memmap2::Mmap::map(&postings_file).unwrap() };
        let index_data = fs::read(&files.postings_index).unwrap();
        let term_dict = TermDict::open(&files.terms).unwrap();

        // Read fieldnorms
        let norm_bytes = fs::read(&files.fieldnorms).unwrap();
        let norm_table = scorer::build_field_norm_table();
        let fieldnorms: Vec<u32> = norm_bytes.iter().map(|&b| norm_table[b as usize]).collect();

        TestIndex {
            _dir: dir,
            postings_mmap,
            index_data,
            term_dict,
            meta,
            fieldnorms,
        }
    }

    fn make_scorer<'a>(
        idx: &'a TestIndex,
        term: &str,
    ) -> Option<TermScorer<'a>> {
        let term_id = idx.term_dict.lookup(term)?;
        let offset = term_id as usize * PostingsIndexEntry::SIZE;
        let entry = PostingsIndexEntry::from_bytes(&idx.index_data[offset..]);
        let iter = TermPostingIterator::new(&idx.postings_mmap, &entry);
        let idf = scorer::idf(entry.doc_freq, idx.meta.doc_count);

        Some(TermScorer {
            iter: Box::new(iter),
            idf,
            avgdl: idx.meta.avgdl,
            cur_doc: NO_MORE_DOCS,
        })
    }

    #[test]
    fn test_single_term_bmw() {
        let idx = create_index("single", vec![
            json!({"body": "rust programming language"}),
            json!({"body": "python scripting language"}),
            json!({"body": "rust is fast and safe"}),
        ]);

        let mut scorers = vec![make_scorer(&idx, "rust").unwrap()];
        let results = block_max_wand(&mut scorers, &idx.fieldnorms, 10, 0);

        assert_eq!(results.len(), 2); // "rust" appears in doc 0 and 2
        assert!(results[0].score > 0.0);
        // Both results should have doc_id 0 or 2
        let doc_ids: Vec<u32> = results.iter().map(|r| r.doc_id).collect();
        assert!(doc_ids.contains(&0));
        assert!(doc_ids.contains(&2));

        let _ = fs::remove_dir_all(&idx._dir);
    }

    #[test]
    fn test_multi_term_bmw() {
        let idx = create_index("multi", vec![
            json!({"body": "rust programming language"}),
            json!({"body": "rust fast systems"}),
            json!({"body": "python scripting"}),
            json!({"body": "rust fast programming"}),
        ]);

        let mut scorers = vec![
            make_scorer(&idx, "rust").unwrap(),
            make_scorer(&idx, "fast").unwrap(),
        ];
        let results = block_max_wand(&mut scorers, &idx.fieldnorms, 10, 0);

        // "rust" + "fast" both in docs 1 and 3
        // Doc 3 has "rust fast programming" — both terms
        assert!(!results.is_empty());
        // Top result should be a doc with both terms
        let top_doc = results[0].doc_id;
        assert!(top_doc == 1 || top_doc == 3);

        let _ = fs::remove_dir_all(&idx._dir);
    }

    #[test]
    fn test_bmw_top_k_limit() {
        let docs: Vec<_> = (0..50)
            .map(|i| json!({"body": format!("search term doc{}", i)}))
            .collect();
        let idx = create_index("topk", docs);

        let mut scorers = vec![make_scorer(&idx, "search").unwrap()];
        let results = block_max_wand(&mut scorers, &idx.fieldnorms, 5, 0);

        assert_eq!(results.len(), 5);
        // Results should be sorted by score descending
        for i in 1..results.len() {
            assert!(results[i - 1].score >= results[i].score);
        }

        let _ = fs::remove_dir_all(&idx._dir);
    }

    #[test]
    fn test_bmw_matches_exhaustive() {
        let docs: Vec<_> = (0..100)
            .map(|i| {
                let body = if i % 3 == 0 {
                    format!("alpha beta doc{}", i)
                } else if i % 3 == 1 {
                    format!("alpha gamma doc{}", i)
                } else {
                    format!("beta gamma doc{}", i)
                };
                json!({"body": body})
            })
            .collect();
        let idx = create_index("bmw_vs_exhaustive", docs);

        // BMW results for "alpha"
        let mut scorers = vec![make_scorer(&idx, "alpha").unwrap()];
        let bmw_results = block_max_wand(&mut scorers, &idx.fieldnorms, 10, 0);

        // Exhaustive results for "alpha"
        let term_id = idx.term_dict.lookup("alpha").unwrap();
        let offset = term_id as usize * PostingsIndexEntry::SIZE;
        let entry = PostingsIndexEntry::from_bytes(&idx.index_data[offset..]);
        let mut iter = TermPostingIterator::new(&idx.postings_mmap, &entry);
        let idf = scorer::idf(entry.doc_freq, idx.meta.doc_count);
        let exhaustive_results =
            exhaustive_top_k(&mut iter, &idx.fieldnorms, idf, idx.meta.avgdl, 10, 0);

        // Same number of results
        assert_eq!(bmw_results.len(), exhaustive_results.len());

        // Same doc_ids (order might differ if scores are tied)
        let mut bmw_ids: Vec<u32> = bmw_results.iter().map(|r| r.doc_id).collect();
        let mut exh_ids: Vec<u32> = exhaustive_results.iter().map(|r| r.doc_id).collect();
        bmw_ids.sort();
        exh_ids.sort();
        assert_eq!(bmw_ids, exh_ids);

        let _ = fs::remove_dir_all(&idx._dir);
    }

    #[test]
    fn test_bmw_no_results() {
        let idx = create_index("no_results", vec![
            json!({"body": "hello world"}),
        ]);

        let scorer = make_scorer(&idx, "nonexistent");
        assert!(scorer.is_none());

        let _ = fs::remove_dir_all(&idx._dir);
    }

    #[test]
    fn test_bmw_empty_k() {
        let idx = create_index("empty_k", vec![
            json!({"body": "hello world"}),
        ]);

        let mut scorers = vec![make_scorer(&idx, "hello").unwrap()];
        let results = block_max_wand(&mut scorers, &idx.fieldnorms, 0, 0);
        assert!(results.is_empty());

        let _ = fs::remove_dir_all(&idx._dir);
    }
}
