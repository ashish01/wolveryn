/// Index writer: accumulates documents in memory and flushes to disk segments.

use crate::analyzer;
use crate::scorer;
use crate::segment::*;
use crate::vbyte;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

/// Maximum documents per in-memory buffer before forcing a flush.
const MAX_BUFFER_DOCS: usize = 500_000;

/// A posting: (doc_id, term_frequency)
type Posting = (u32, u32);

/// In-memory index buffer that accumulates documents before flushing.
pub struct IndexWriter {
    /// Term → list of (doc_id, term_freq)
    postings: HashMap<String, Vec<Posting>>,
    /// Per-document field lengths (doc_id → total token count)
    doc_lengths: Vec<u32>,
    /// Stored documents (doc_id → JSON bytes)
    stored_docs: Vec<Vec<u8>>,
    /// Fields to index
    fields: Vec<String>,
    /// Global doc_id offset (for multi-segment indexing)
    base_doc_id: u32,
    /// Number of docs in this buffer
    doc_count: u32,
    /// Total tokens across all docs in buffer
    total_tokens: u64,
}

impl IndexWriter {
    /// Create a new index writer.
    /// - `fields`: which JSON fields to tokenize and index
    /// - `base_doc_id`: starting doc_id for this batch
    pub fn new(fields: Vec<String>, base_doc_id: u32) -> Self {
        Self {
            postings: HashMap::new(),
            doc_lengths: Vec::new(),
            stored_docs: Vec::new(),
            fields,
            base_doc_id,
            doc_count: 0,
            total_tokens: 0,
        }
    }

    /// Add a document from a JSON value.
    /// Returns the assigned doc_id.
    pub fn add_document(&mut self, doc: &serde_json::Value) -> u32 {
        let doc_id = self.base_doc_id + self.doc_count;
        let mut term_counts: HashMap<String, u32> = HashMap::new();
        let mut doc_len: u32 = 0;

        // Tokenize each configured field
        for field in &self.fields {
            if let Some(text) = doc.get(field).and_then(|v| v.as_str()) {
                let tokens = analyzer::tokenize_terms(text);
                doc_len += tokens.len() as u32;
                for term in tokens {
                    *term_counts.entry(term).or_insert(0) += 1;
                }
            }
        }

        // Add to postings
        for (term, tf) in term_counts {
            self.postings
                .entry(term)
                .or_insert_with(Vec::new)
                .push((doc_id, tf));
        }

        // Store field lengths
        self.doc_lengths.push(doc_len);
        self.total_tokens += doc_len as u64;

        // Store original document
        let doc_bytes = serde_json::to_vec(doc).unwrap_or_default();
        self.stored_docs.push(doc_bytes);

        self.doc_count += 1;
        doc_id
    }

    /// Number of documents currently buffered.
    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }

    /// Whether the buffer should be flushed.
    pub fn should_flush(&self) -> bool {
        self.doc_count as usize >= MAX_BUFFER_DOCS
    }

    /// Next doc_id that would be assigned.
    pub fn next_doc_id(&self) -> u32 {
        self.base_doc_id + self.doc_count
    }

    /// Flush the in-memory buffer to a segment on disk.
    /// Returns the segment metadata.
    pub fn flush_segment(&mut self, segment_dir: &Path) -> std::io::Result<SegmentMeta> {
        fs::create_dir_all(segment_dir)?;
        let files = segment_files(segment_dir);

        let avgdl = if self.doc_count > 0 {
            self.total_tokens as f32 / self.doc_count as f32
        } else {
            1.0
        };

        // 1. Sort terms alphabetically
        let mut sorted_terms: Vec<String> = self.postings.keys().cloned().collect();
        sorted_terms.sort();

        let term_count = sorted_terms.len() as u32;
        let total_docs = self.doc_count; // for IDF computation within this segment

        // 2. Build terms.bin
        self.write_terms_bin(&files.terms, &sorted_terms)?;

        // 3. Build postings.bin and postings_index.bin
        self.write_postings(&files.postings, &files.postings_index, &sorted_terms, avgdl, total_docs)?;

        // 4. Write fieldnorms.bin
        self.write_fieldnorms(&files.fieldnorms)?;

        // 5. Write docs.bin and docs_index.bin
        self.write_docs(&files.docs, &files.docs_index)?;

        // 6. Write meta.json
        let meta = SegmentMeta {
            doc_count: self.doc_count,
            total_tokens: self.total_tokens,
            avgdl,
            min_doc_id: self.base_doc_id,
            max_doc_id: if self.doc_count > 0 {
                self.base_doc_id + self.doc_count - 1
            } else {
                self.base_doc_id
            },
            term_count,
            fields: self.fields.clone(),
        };
        let meta_json = serde_json::to_string_pretty(&meta)?;
        fs::write(&files.meta, meta_json)?;

        // Clear buffer for reuse
        let next_base = self.next_doc_id();
        self.postings.clear();
        self.doc_lengths.clear();
        self.stored_docs.clear();
        self.doc_count = 0;
        self.total_tokens = 0;
        self.base_doc_id = next_base;

        Ok(meta)
    }

    /// Write terms.bin: [term_count: u32][offset_table][term_bytes]
    fn write_terms_bin(&self, path: &Path, sorted_terms: &[String]) -> std::io::Result<()> {
        let mut file = fs::File::create(path)?;
        let term_count = sorted_terms.len() as u32;

        // Write term count
        file.write_all(&term_count.to_le_bytes())?;

        // Build offset table and concatenated term bytes
        let mut term_bytes = Vec::new();
        let mut offset_entries = Vec::new();

        for term in sorted_terms {
            let term_b = term.as_bytes();
            let entry = TermOffsetEntry {
                offset: term_bytes.len() as u32,
                len: term_b.len() as u16,
            };
            offset_entries.push(entry);
            term_bytes.extend_from_slice(term_b);
        }

        // Write offset table
        for entry in &offset_entries {
            file.write_all(&entry.to_bytes())?;
        }

        // Write term bytes
        file.write_all(&term_bytes)?;

        Ok(())
    }

    /// Write postings.bin and postings_index.bin with block structure.
    fn write_postings(
        &mut self,
        postings_path: &Path,
        index_path: &Path,
        sorted_terms: &[String],
        avgdl: f32,
        total_docs: u32,
    ) -> std::io::Result<()> {
        let mut postings_file = fs::File::create(postings_path)?;
        let mut index_file = fs::File::create(index_path)?;
        let mut postings_offset: u64 = 0;

        for term in sorted_terms {
            let term_postings = self.postings.get_mut(term).unwrap();
            // Sort postings by doc_id
            term_postings.sort_by_key(|&(doc_id, _)| doc_id);

            let doc_freq = term_postings.len() as u32;
            let idf_val = scorer::idf(doc_freq, total_docs);

            // Write postings index entry
            let idx_entry = PostingsIndexEntry {
                offset: postings_offset,
                doc_freq,
            };
            index_file.write_all(&idx_entry.to_bytes())?;

            // Write block count
            let num_blocks = (term_postings.len() + BLOCK_SIZE - 1) / BLOCK_SIZE;
            let block_count_bytes = (num_blocks as u32).to_le_bytes();
            postings_file.write_all(&block_count_bytes)?;
            postings_offset += 4;

            // Write blocks
            for block_idx in 0..num_blocks {
                let start = block_idx * BLOCK_SIZE;
                let end = (start + BLOCK_SIZE).min(term_postings.len());
                let block = &term_postings[start..end];

                // Compute block-max score
                let mut max_tf: u32 = 0;
                let mut min_dl: u32 = u32::MAX;
                for &(doc_id, tf) in block {
                    max_tf = max_tf.max(tf);
                    let local_id = (doc_id - self.base_doc_id) as usize;
                    let dl = self.doc_lengths[local_id];
                    min_dl = min_dl.min(dl.max(1)); // avoid 0
                }
                let block_max = scorer::max_block_score(max_tf, min_dl, avgdl, idf_val);

                // Delta-encode postings within this block
                // Base doc_id for first block is 0, for subsequent blocks it's
                // the previous block's last doc_id (but we encode relative within block)
                let base = if block_idx == 0 { 0u32 } else { term_postings[start - 1].0 };
                let relative_postings: Vec<(u32, u32)> = block
                    .iter()
                    .map(|&(doc_id, tf)| (doc_id - base, tf))
                    .collect();

                let mut compressed = Vec::new();
                vbyte::encode_postings(&relative_postings, &mut compressed);

                let last_doc_id = block.last().unwrap().0;
                let header = BlockHeader {
                    last_doc_id,
                    max_block_score: block_max,
                    byte_len: compressed.len() as u16,
                };

                postings_file.write_all(&header.to_bytes())?;
                postings_file.write_all(&compressed)?;
                postings_offset += BlockHeader::SIZE as u64 + compressed.len() as u64;
            }
        }

        Ok(())
    }

    /// Write fieldnorms.bin: one byte per document.
    fn write_fieldnorms(&self, path: &Path) -> std::io::Result<()> {
        let mut norms = Vec::with_capacity(self.doc_lengths.len());
        for &dl in &self.doc_lengths {
            norms.push(scorer::encode_field_length(dl));
        }
        fs::write(path, &norms)
    }

    /// Write docs.bin and docs_index.bin.
    fn write_docs(&self, docs_path: &Path, index_path: &Path) -> std::io::Result<()> {
        let mut docs_file = fs::File::create(docs_path)?;
        let mut index_file = fs::File::create(index_path)?;
        let mut offset: u64 = 0;

        for doc_bytes in &self.stored_docs {
            let entry = DocIndexEntry {
                offset,
                length: doc_bytes.len() as u32,
            };
            index_file.write_all(&entry.to_bytes())?;
            docs_file.write_all(doc_bytes)?;
            offset += doc_bytes.len() as u64;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("search_test_{}_{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_add_documents() {
        let mut writer = IndexWriter::new(vec!["title".into(), "body".into()], 0);

        writer.add_document(&json!({
            "title": "Hello World",
            "body": "This is a test document"
        }));
        writer.add_document(&json!({
            "title": "Rust Programming",
            "body": "Rust is fast and safe"
        }));
        writer.add_document(&json!({
            "title": "Search Engine",
            "body": "Building a search engine from scratch"
        }));

        assert_eq!(writer.doc_count(), 3);
        assert!(writer.postings.contains_key("hello"));
        assert!(writer.postings.contains_key("rust"));
        assert!(writer.postings.contains_key("search"));
    }

    #[test]
    fn test_term_frequencies() {
        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({
            "body": "rust rust rust programming"
        }));

        let rust_postings = writer.postings.get("rust").unwrap();
        assert_eq!(rust_postings.len(), 1);
        assert_eq!(rust_postings[0], (0, 3)); // doc_id=0, tf=3
    }

    #[test]
    fn test_field_lengths() {
        let mut writer = IndexWriter::new(vec!["title".into(), "body".into()], 0);
        writer.add_document(&json!({
            "title": "Hello",
            "body": "World is great"
        }));
        // title has 1 token, body has 3 tokens → total 4
        assert_eq!(writer.doc_lengths[0], 4);
    }

    #[test]
    fn test_flush_creates_all_files() {
        let dir = test_dir("flush_creates");
        let seg_dir = dir.join("segment_0");

        let mut writer = IndexWriter::new(vec!["title".into(), "body".into()], 0);
        writer.add_document(&json!({"title": "Hello World", "body": "Test document"}));
        writer.add_document(&json!({"title": "Rust Search", "body": "Fast search engine"}));
        writer.add_document(&json!({"title": "Go Web", "body": "Web server in Go"}));

        let meta = writer.flush_segment(&seg_dir).unwrap();

        assert_eq!(meta.doc_count, 3);
        assert_eq!(meta.min_doc_id, 0);
        assert_eq!(meta.max_doc_id, 2);
        assert!(meta.avgdl > 0.0);
        assert!(meta.term_count > 0);

        let files = segment_files(&seg_dir);
        assert!(files.meta.exists());
        assert!(files.terms.exists());
        assert!(files.postings.exists());
        assert!(files.postings_index.exists());
        assert!(files.fieldnorms.exists());
        assert!(files.docs.exists());
        assert!(files.docs_index.exists());

        // Verify fieldnorms has 3 bytes (one per doc)
        let norms = fs::read(&files.fieldnorms).unwrap();
        assert_eq!(norms.len(), 3);

        // Verify meta.json is valid
        let meta_str = fs::read_to_string(&files.meta).unwrap();
        let meta2: SegmentMeta = serde_json::from_str(&meta_str).unwrap();
        assert_eq!(meta2.doc_count, 3);

        // Verify postings_index has entries
        let idx_data = fs::read(&files.postings_index).unwrap();
        assert_eq!(idx_data.len() % PostingsIndexEntry::SIZE, 0);
        let num_terms = idx_data.len() / PostingsIndexEntry::SIZE;
        assert_eq!(num_terms as u32, meta.term_count);

        // Verify docs_index has 3 entries
        let docs_idx = fs::read(&files.docs_index).unwrap();
        assert_eq!(docs_idx.len(), 3 * DocIndexEntry::SIZE);

        // Cleanup
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_flush_resets_buffer() {
        let dir = test_dir("flush_resets");
        let seg_dir = dir.join("segment_0");

        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({"body": "hello"}));
        writer.flush_segment(&seg_dir).unwrap();

        assert_eq!(writer.doc_count(), 0);
        assert_eq!(writer.next_doc_id(), 1); // base advances
        assert!(writer.postings.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_terms_sorted_in_file() {
        let dir = test_dir("terms_sorted");
        let seg_dir = dir.join("segment_0");

        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({"body": "zebra apple mango"}));
        writer.flush_segment(&seg_dir).unwrap();

        // Read terms.bin and verify sorted order
        let data = fs::read(seg_dir.join(TERMS_FILE)).unwrap();
        let term_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        assert_eq!(term_count, 3);

        let offset_table_start = 4;
        let mut terms = Vec::new();
        let term_bytes_start = offset_table_start + term_count * TermOffsetEntry::SIZE;

        for i in 0..term_count {
            let entry_start = offset_table_start + i * TermOffsetEntry::SIZE;
            let entry = TermOffsetEntry::from_bytes(&data[entry_start..]);
            let abs_offset = term_bytes_start + entry.offset as usize;
            let term = std::str::from_utf8(&data[abs_offset..abs_offset + entry.len as usize]).unwrap();
            terms.push(term.to_string());
        }

        assert_eq!(terms, vec!["apple", "mango", "zebra"]);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_block_structure() {
        let dir = test_dir("block_structure");
        let seg_dir = dir.join("segment_0");

        // Create enough docs that we get multiple blocks for a common term
        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        for i in 0..200 {
            writer.add_document(&json!({"body": format!("common term doc{}", i)}));
        }
        let meta = writer.flush_segment(&seg_dir).unwrap();
        assert_eq!(meta.doc_count, 200);

        // Read postings for "common" — should have 2 blocks (200/128 = 1 full + 1 partial)
        let postings_data = fs::read(seg_dir.join(POSTINGS_FILE)).unwrap();
        let index_data = fs::read(seg_dir.join(POSTINGS_INDEX_FILE)).unwrap();

        // Find "common" term_id by reading terms.bin
        let terms_data = fs::read(seg_dir.join(TERMS_FILE)).unwrap();
        let term_count = u32::from_le_bytes(terms_data[0..4].try_into().unwrap()) as usize;
        let term_bytes_start = 4 + term_count * TermOffsetEntry::SIZE;

        let mut common_term_id = None;
        for i in 0..term_count {
            let entry_start = 4 + i * TermOffsetEntry::SIZE;
            let entry = TermOffsetEntry::from_bytes(&terms_data[entry_start..]);
            let abs_offset = term_bytes_start + entry.offset as usize;
            let term = std::str::from_utf8(&terms_data[abs_offset..abs_offset + entry.len as usize]).unwrap();
            if term == "common" {
                common_term_id = Some(i);
                break;
            }
        }
        let term_id = common_term_id.expect("term 'common' not found");

        // Read postings index entry
        let idx_entry_start = term_id * PostingsIndexEntry::SIZE;
        let idx_entry = PostingsIndexEntry::from_bytes(&index_data[idx_entry_start..]);
        assert_eq!(idx_entry.doc_freq, 200);

        // Read block count from postings
        let offset = idx_entry.offset as usize;
        let block_count = u32::from_le_bytes(postings_data[offset..offset + 4].try_into().unwrap());
        assert_eq!(block_count, 2); // 200 docs = 128 + 72

        // Read first block header
        let hdr = BlockHeader::from_bytes(&postings_data[offset + 4..]);
        assert_eq!(hdr.last_doc_id, 127); // 0-indexed, block of 128
        assert!(hdr.max_block_score > 0.0);
        assert!(hdr.byte_len > 0);

        let _ = fs::remove_dir_all(&dir);
    }
}
