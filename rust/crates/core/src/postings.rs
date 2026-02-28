/// Postings reader and TermIterator.
///
/// Reads block-structured postings from memory-mapped files.
/// Each term's postings consist of:
///   [block_count: u32]
///   For each block:
///     [BlockHeader: last_doc_id(4) + max_block_score(4) + byte_len(2) = 10 bytes]
///     [compressed postings: vbyte-encoded (doc_gap, tf) pairs]

use crate::segment::{BlockHeader, PostingsIndexEntry, BLOCK_SIZE};
use crate::vbyte;

/// The core iterator trait for query evaluation.
/// All boolean operators and term iterators implement this.
pub trait PostingIterator {
    /// Advance to the next document. Returns the doc_id or None if exhausted.
    fn next_doc(&mut self) -> Option<u32>;

    /// Skip forward to the first doc_id >= target. Returns doc_id or None.
    fn advance(&mut self, target: u32) -> Option<u32>;

    /// Move to the block containing `target` without decoding postings.
    /// Updates max_block_score(). Returns the block's last_doc_id or None.
    fn shallow_advance(&mut self, target: u32) -> Option<u32>;

    /// Current document id. Only valid after next_doc() or advance() returns Some.
    fn doc_id(&self) -> u32;

    /// Last doc_id in the current block.
    fn block_max_doc(&self) -> u32;

    /// Upper-bound BM25 contribution for any doc in the current block.
    fn max_block_score(&self) -> f32;

    /// Term frequency in the current document.
    fn term_freq(&self) -> u32;

    /// Total documents containing this term.
    fn doc_freq(&self) -> u32;
}

/// Sentinel value for "no current document".
pub const NO_MORE_DOCS: u32 = u32::MAX;

/// Iterator over a single term's postings from a segment.
/// Block-aware: reads block headers for shallow_advance, decodes vbyte on demand.
pub struct TermPostingIterator<'a> {
    /// Memory-mapped postings data for the entire segment.
    data: &'a [u8],
    /// Document frequency.
    df: u32,
    /// Byte offset of the block_count field for this term.
    #[allow(dead_code)]
    term_start: usize,
    /// Number of blocks.
    block_count: u32,
    /// Current block index (0-based).
    current_block: u32,
    /// Decoded postings in the current block: (doc_id, tf).
    current_block_postings: Vec<(u32, u32)>,
    /// Position within current_block_postings.
    pos_in_block: usize,
    /// Current block header.
    current_header: BlockHeader,
    /// Byte offset of the current block's header within `data`.
    block_offsets: Vec<usize>,
    /// Whether we've initialized (read block offsets).
    #[allow(dead_code)]
    initialized: bool,
    /// Current doc_id.
    cur_doc_id: u32,
    /// Current term frequency.
    cur_tf: u32,
    /// Whether the current block's postings are decoded.
    block_decoded: bool,
    // Block-max scores are precomputed in headers at index time.
}

impl<'a> TermPostingIterator<'a> {
    /// Create a new iterator for a term.
    /// - `data`: the entire postings.bin mmap
    /// - `entry`: the PostingsIndexEntry for this term
    pub fn new(data: &'a [u8], entry: &PostingsIndexEntry) -> Self {
        let term_start = entry.offset as usize;
        let block_count = u32::from_le_bytes(
            data[term_start..term_start + 4].try_into().unwrap(),
        );

        // Pre-compute block header offsets for O(1) access
        let mut block_offsets = Vec::with_capacity(block_count as usize);
        let mut offset = term_start + 4; // skip block_count
        for _ in 0..block_count {
            block_offsets.push(offset);
            let header = BlockHeader::from_bytes(&data[offset..]);
            offset += BlockHeader::SIZE + header.byte_len as usize;
        }

        let initial_header = if block_count > 0 {
            BlockHeader::from_bytes(&data[block_offsets[0]..])
        } else {
            BlockHeader {
                last_doc_id: NO_MORE_DOCS,
                max_block_score: 0.0,
                byte_len: 0,
            }
        };

        Self {
            data,
            df: entry.doc_freq,
            term_start,
            block_count,
            current_block: 0,
            current_block_postings: Vec::new(),
            pos_in_block: 0,
            current_header: initial_header,
            block_offsets,
            initialized: true,
            cur_doc_id: NO_MORE_DOCS,
            cur_tf: 0,
            block_decoded: false,
        }
    }

    /// Decode the current block's compressed postings.
    fn decode_current_block(&mut self) {
        if self.current_block >= self.block_count {
            self.current_block_postings.clear();
            self.block_decoded = true;
            return;
        }

        let offset = self.block_offsets[self.current_block as usize];
        let header = BlockHeader::from_bytes(&self.data[offset..]);
        self.current_header = header;

        let postings_start = offset + BlockHeader::SIZE;
        let postings_end = postings_start + header.byte_len as usize;
        let compressed = &self.data[postings_start..postings_end];

        // Determine doc count in this block
        let total_so_far = self.current_block as usize * BLOCK_SIZE;
        let remaining = self.df as usize - total_so_far;
        let docs_in_block = remaining.min(BLOCK_SIZE);

        // Base doc_id: for first block it's 0, for subsequent blocks
        // it's the previous block's last_doc_id
        let base = if self.current_block == 0 {
            0u32
        } else {
            let prev_offset = self.block_offsets[(self.current_block - 1) as usize];
            let prev_header = BlockHeader::from_bytes(&self.data[prev_offset..]);
            prev_header.last_doc_id
        };

        let (postings, _) = vbyte::decode_postings(compressed, docs_in_block, base);
        self.current_block_postings = postings;
        self.pos_in_block = 0;
        self.block_decoded = true;
    }

    /// Move to the next block.
    fn advance_block(&mut self) {
        self.current_block += 1;
        self.block_decoded = false;
        self.pos_in_block = 0;
        if self.current_block < self.block_count {
            let offset = self.block_offsets[self.current_block as usize];
            self.current_header = BlockHeader::from_bytes(&self.data[offset..]);
        } else {
            self.current_header = BlockHeader {
                last_doc_id: NO_MORE_DOCS,
                max_block_score: 0.0,
                byte_len: 0,
            };
        }
    }
}

impl<'a> PostingIterator for TermPostingIterator<'a> {
    fn next_doc(&mut self) -> Option<u32> {
        if self.current_block >= self.block_count {
            self.cur_doc_id = NO_MORE_DOCS;
            return None;
        }

        if !self.block_decoded {
            self.decode_current_block();
        }

        // Try next position in current block
        if self.pos_in_block < self.current_block_postings.len() {
            let (doc_id, tf) = self.current_block_postings[self.pos_in_block];
            self.cur_doc_id = doc_id;
            self.cur_tf = tf;
            self.pos_in_block += 1;
            return Some(doc_id);
        }

        // Move to next block
        self.advance_block();
        if self.current_block >= self.block_count {
            self.cur_doc_id = NO_MORE_DOCS;
            return None;
        }

        self.decode_current_block();
        if self.pos_in_block < self.current_block_postings.len() {
            let (doc_id, tf) = self.current_block_postings[self.pos_in_block];
            self.cur_doc_id = doc_id;
            self.cur_tf = tf;
            self.pos_in_block += 1;
            return Some(doc_id);
        }

        self.cur_doc_id = NO_MORE_DOCS;
        None
    }

    fn advance(&mut self, target: u32) -> Option<u32> {
        // If already positioned at a doc >= target, return it
        if self.cur_doc_id != NO_MORE_DOCS && self.cur_doc_id >= target {
            return Some(self.cur_doc_id);
        }

        // Skip blocks whose last_doc_id < target
        while self.current_block < self.block_count {
            if self.current_header.last_doc_id >= target {
                break;
            }
            self.advance_block();
        }

        if self.current_block >= self.block_count {
            self.cur_doc_id = NO_MORE_DOCS;
            return None;
        }

        // Decode this block if needed
        if !self.block_decoded {
            self.decode_current_block();
        }

        // Linear scan within block for doc_id >= target
        while self.pos_in_block < self.current_block_postings.len() {
            let (doc_id, tf) = self.current_block_postings[self.pos_in_block];
            if doc_id >= target {
                self.cur_doc_id = doc_id;
                self.cur_tf = tf;
                self.pos_in_block += 1;
                return Some(doc_id);
            }
            self.pos_in_block += 1;
        }

        // Try next blocks
        self.advance_block();
        self.advance(target)
    }

    fn shallow_advance(&mut self, target: u32) -> Option<u32> {
        // Skip blocks using only headers (no decoding)
        while self.current_block < self.block_count {
            if self.current_header.last_doc_id >= target {
                self.block_decoded = false;
                return Some(self.current_header.last_doc_id);
            }
            self.current_block += 1;
            if self.current_block < self.block_count {
                let offset = self.block_offsets[self.current_block as usize];
                self.current_header = BlockHeader::from_bytes(&self.data[offset..]);
            }
        }

        self.current_header = BlockHeader {
            last_doc_id: NO_MORE_DOCS,
            max_block_score: 0.0,
            byte_len: 0,
        };
        None
    }

    fn doc_id(&self) -> u32 {
        self.cur_doc_id
    }

    fn block_max_doc(&self) -> u32 {
        self.current_header.last_doc_id
    }

    fn max_block_score(&self) -> f32 {
        self.current_header.max_block_score
    }

    fn term_freq(&self) -> u32 {
        self.cur_tf
    }

    fn doc_freq(&self) -> u32 {
        self.df
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_writer::IndexWriter;
    use crate::segment::{self, PostingsIndexEntry};
    use crate::term_dict::TermDict;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("search_test_post_{}_{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    struct TestSegment {
        _dir: PathBuf,
        postings_mmap: memmap2::Mmap,
        index_data: Vec<u8>,
        term_dict: TermDict,
    }

    fn create_segment(name: &str, docs: Vec<serde_json::Value>) -> TestSegment {
        let dir = test_dir(name);
        let seg_dir = dir.join("segment_0");

        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        for doc in &docs {
            writer.add_document(doc);
        }
        writer.flush_segment(&seg_dir).unwrap();

        let files = segment::segment_files(&seg_dir);
        let postings_file = fs::File::open(&files.postings).unwrap();
        let postings_mmap = unsafe { memmap2::Mmap::map(&postings_file).unwrap() };
        let index_data = fs::read(&files.postings_index).unwrap();
        let term_dict = TermDict::open(&files.terms).unwrap();

        TestSegment {
            _dir: dir,
            postings_mmap,
            index_data,
            term_dict,
        }
    }

    fn get_entry(seg: &TestSegment, term: &str) -> PostingsIndexEntry {
        let term_id = seg.term_dict.lookup(term).expect("term not found");
        let offset = term_id as usize * PostingsIndexEntry::SIZE;
        PostingsIndexEntry::from_bytes(&seg.index_data[offset..])
    }

    #[test]
    fn test_iterate_all_docs() {
        let seg = create_segment("iter_all", vec![
            json!({"body": "hello world"}),
            json!({"body": "hello rust"}),
            json!({"body": "hello search"}),
        ]);
        let entry = get_entry(&seg, "hello");
        assert_eq!(entry.doc_freq, 3);

        let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
        assert_eq!(iter.next_doc(), Some(0));
        assert_eq!(iter.term_freq(), 1);
        assert_eq!(iter.next_doc(), Some(1));
        assert_eq!(iter.next_doc(), Some(2));
        assert_eq!(iter.next_doc(), None);

        let _ = fs::remove_dir_all(&seg._dir);
    }

    #[test]
    fn test_advance() {
        let seg = create_segment("advance", vec![
            json!({"body": "a b c"}),
            json!({"body": "a d e"}),
            json!({"body": "f g h"}),
            json!({"body": "a i j"}),
            json!({"body": "a k l"}),
        ]);
        let entry = get_entry(&seg, "a");
        assert_eq!(entry.doc_freq, 4); // docs 0,1,3,4

        let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
        // Skip to doc >= 2
        assert_eq!(iter.advance(2), Some(3));
        assert_eq!(iter.doc_id(), 3);

        // Skip to doc >= 4
        assert_eq!(iter.advance(4), Some(4));
        assert_eq!(iter.doc_id(), 4);

        // No more
        assert_eq!(iter.advance(5), None);

        let _ = fs::remove_dir_all(&seg._dir);
    }

    #[test]
    fn test_advance_to_exact() {
        let seg = create_segment("advance_exact", vec![
            json!({"body": "hello"}),
            json!({"body": "world"}),
            json!({"body": "hello"}),
        ]);
        let entry = get_entry(&seg, "hello");
        let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);

        assert_eq!(iter.advance(0), Some(0));
        assert_eq!(iter.advance(2), Some(2));
        assert_eq!(iter.advance(3), None);

        let _ = fs::remove_dir_all(&seg._dir);
    }

    #[test]
    fn test_shallow_advance() {
        // Create enough docs for multiple blocks
        let mut docs = Vec::new();
        for i in 0..200 {
            docs.push(json!({"body": format!("common term doc{}", i)}));
        }
        let seg = create_segment("shallow", docs);
        let entry = get_entry(&seg, "common");

        let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);

        // Shallow advance to block containing doc 130
        let result = iter.shallow_advance(130);
        assert!(result.is_some());
        let block_max_doc = result.unwrap();
        assert!(block_max_doc >= 130, "block_max_doc {} should be >= 130", block_max_doc);
        assert!(iter.max_block_score() > 0.0);

        let _ = fs::remove_dir_all(&seg._dir);
    }

    #[test]
    fn test_block_boundaries() {
        // Exactly 128 docs → 1 block
        let docs: Vec<_> = (0..128).map(|i| json!({"body": format!("term{}", i % 5)})).collect();
        let seg = create_segment("block_boundary", docs);

        // "term0" appears in docs 0,5,10,...,125 = 26 docs, all in block 0
        let entry = get_entry(&seg, "term0");
        let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);

        let mut count = 0;
        while iter.next_doc().is_some() {
            count += 1;
        }
        assert_eq!(count, entry.doc_freq as usize);

        let _ = fs::remove_dir_all(&seg._dir);
    }

    #[test]
    fn test_term_freq_values() {
        let seg = create_segment("tf_values", vec![
            json!({"body": "rust rust rust"}),     // tf=3
            json!({"body": "rust"}),                // tf=1
            json!({"body": "rust rust rust rust rust"}), // tf=5
        ]);
        let entry = get_entry(&seg, "rust");
        let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);

        assert_eq!(iter.next_doc(), Some(0));
        assert_eq!(iter.term_freq(), 3);
        assert_eq!(iter.next_doc(), Some(1));
        assert_eq!(iter.term_freq(), 1);
        assert_eq!(iter.next_doc(), Some(2));
        assert_eq!(iter.term_freq(), 5);

        let _ = fs::remove_dir_all(&seg._dir);
    }

    #[test]
    fn test_multi_block_iteration() {
        // 300 docs, all containing "common" → 3 blocks (128+128+44)
        let docs: Vec<_> = (0..300).map(|_| json!({"body": "common word here"})).collect();
        let seg = create_segment("multi_block", docs);
        let entry = get_entry(&seg, "common");
        assert_eq!(entry.doc_freq, 300);

        let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
        let mut doc_ids = Vec::new();
        while let Some(id) = iter.next_doc() {
            doc_ids.push(id);
        }
        assert_eq!(doc_ids.len(), 300);
        // Should be 0..300 in order
        for (i, &id) in doc_ids.iter().enumerate() {
            assert_eq!(id, i as u32);
        }

        let _ = fs::remove_dir_all(&seg._dir);
    }
}
