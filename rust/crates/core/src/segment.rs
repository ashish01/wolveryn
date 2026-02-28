/// Segment format definitions and metadata.
///
/// Each segment is a directory containing:
/// - meta.json     — segment metadata
/// - terms.bin     — sorted term dictionary
/// - postings.bin  — block-structured postings
/// - postings_index.bin — term_id → postings offset
/// - fieldnorms.bin     — quantized field lengths
/// - docs.bin      — stored original documents
/// - docs_index.bin     — doc_id → document offset

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Number of documents per postings block.
pub const BLOCK_SIZE: usize = 128;

/// File names within a segment directory.
pub const META_FILE: &str = "meta.json";
pub const TERMS_FILE: &str = "terms.bin";
pub const POSTINGS_FILE: &str = "postings.bin";
pub const POSTINGS_INDEX_FILE: &str = "postings_index.bin";
pub const FIELDNORMS_FILE: &str = "fieldnorms.bin";
pub const DOCS_FILE: &str = "docs.bin";
pub const DOCS_INDEX_FILE: &str = "docs_index.bin";

/// Segment metadata, serialized as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Number of documents in this segment.
    pub doc_count: u32,
    /// Total number of tokens across all documents (for avgdl).
    pub total_tokens: u64,
    /// Average document length.
    pub avgdl: f32,
    /// First document ID in this segment (global).
    pub min_doc_id: u32,
    /// Last document ID in this segment (global).
    pub max_doc_id: u32,
    /// Number of unique terms.
    pub term_count: u32,
    /// Names of indexed fields.
    pub fields: Vec<String>,
}

impl SegmentMeta {
    pub fn file_path(segment_dir: &Path) -> PathBuf {
        segment_dir.join(META_FILE)
    }
}

/// Entry in postings_index.bin: maps term_id to its postings data.
/// Layout: [offset: u64 (8 bytes)] [doc_freq: u32 (4 bytes)]
/// Total: 12 bytes per entry.
#[derive(Debug, Clone, Copy)]
pub struct PostingsIndexEntry {
    /// Byte offset into postings.bin where this term's data starts.
    pub offset: u64,
    /// Number of documents containing this term.
    pub doc_freq: u32,
}

impl PostingsIndexEntry {
    pub const SIZE: usize = 12; // 8 + 4

    pub fn to_bytes(&self) -> [u8; 12] {
        let mut buf = [0u8; 12];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.doc_freq.to_le_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            offset: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            doc_freq: u32::from_le_bytes(data[8..12].try_into().unwrap()),
        }
    }
}

/// Entry in docs_index.bin: maps local doc_id to stored document location.
/// Layout: [offset: u64 (8 bytes)] [length: u32 (4 bytes)]
/// Total: 12 bytes per entry.
#[derive(Debug, Clone, Copy)]
pub struct DocIndexEntry {
    /// Byte offset into docs.bin.
    pub offset: u64,
    /// Length of the stored document in bytes.
    pub length: u32,
}

impl DocIndexEntry {
    pub const SIZE: usize = 12; // 8 + 4

    pub fn to_bytes(&self) -> [u8; 12] {
        let mut buf = [0u8; 12];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.length.to_le_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            offset: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            length: u32::from_le_bytes(data[8..12].try_into().unwrap()),
        }
    }
}

/// Block header in postings.bin.
/// Layout: [last_doc_id: u32 (4)] [max_block_score: f32 (4)] [byte_len: u16 (2)]
/// Total: 10 bytes per block header.
#[derive(Debug, Clone, Copy)]
pub struct BlockHeader {
    /// Last (highest) doc_id in this block.
    pub last_doc_id: u32,
    /// Upper-bound BM25 score for any doc in this block.
    pub max_block_score: f32,
    /// Number of bytes of compressed postings following this header.
    pub byte_len: u16,
}

impl BlockHeader {
    pub const SIZE: usize = 10; // 4 + 4 + 2

    pub fn to_bytes(&self) -> [u8; 10] {
        let mut buf = [0u8; 10];
        buf[0..4].copy_from_slice(&self.last_doc_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.max_block_score.to_le_bytes());
        buf[8..10].copy_from_slice(&self.byte_len.to_le_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            last_doc_id: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            max_block_score: f32::from_le_bytes(data[4..8].try_into().unwrap()),
            byte_len: u16::from_le_bytes(data[8..10].try_into().unwrap()),
        }
    }
}

/// terms.bin format:
/// [term_count: u32 (4 bytes)]
/// [offset_table: term_count entries of (offset: u32, len: u16) = 6 bytes each]
/// [term_bytes: concatenated term strings]
///
/// Term offset entry: points into the term_bytes region.
#[derive(Debug, Clone, Copy)]
pub struct TermOffsetEntry {
    /// Byte offset into the term_bytes region (relative to start of term_bytes).
    pub offset: u32,
    /// Length of the term string in bytes.
    pub len: u16,
}

impl TermOffsetEntry {
    pub const SIZE: usize = 6; // 4 + 2

    pub fn to_bytes(&self) -> [u8; 6] {
        let mut buf = [0u8; 6];
        buf[0..4].copy_from_slice(&self.offset.to_le_bytes());
        buf[4..6].copy_from_slice(&self.len.to_le_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            offset: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            len: u16::from_le_bytes(data[4..6].try_into().unwrap()),
        }
    }
}

/// Helper: get all file paths for a segment directory.
pub fn segment_files(dir: &Path) -> SegmentFiles {
    SegmentFiles {
        meta: dir.join(META_FILE),
        terms: dir.join(TERMS_FILE),
        postings: dir.join(POSTINGS_FILE),
        postings_index: dir.join(POSTINGS_INDEX_FILE),
        fieldnorms: dir.join(FIELDNORMS_FILE),
        docs: dir.join(DOCS_FILE),
        docs_index: dir.join(DOCS_INDEX_FILE),
    }
}

#[derive(Debug)]
pub struct SegmentFiles {
    pub meta: PathBuf,
    pub terms: PathBuf,
    pub postings: PathBuf,
    pub postings_index: PathBuf,
    pub fieldnorms: PathBuf,
    pub docs: PathBuf,
    pub docs_index: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_meta_json_roundtrip() {
        let meta = SegmentMeta {
            doc_count: 1000,
            total_tokens: 150_000,
            avgdl: 150.0,
            min_doc_id: 0,
            max_doc_id: 999,
            term_count: 5000,
            fields: vec!["title".into(), "body".into()],
        };
        let json = serde_json::to_string_pretty(&meta).unwrap();
        let decoded: SegmentMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.doc_count, 1000);
        assert_eq!(decoded.total_tokens, 150_000);
        assert_eq!(decoded.fields, vec!["title", "body"]);
    }

    #[test]
    fn test_postings_index_entry_roundtrip() {
        let entry = PostingsIndexEntry {
            offset: 123456789,
            doc_freq: 42,
        };
        let bytes = entry.to_bytes();
        let decoded = PostingsIndexEntry::from_bytes(&bytes);
        assert_eq!(decoded.offset, 123456789);
        assert_eq!(decoded.doc_freq, 42);
    }

    #[test]
    fn test_doc_index_entry_roundtrip() {
        let entry = DocIndexEntry {
            offset: 9876543210,
            length: 1024,
        };
        let bytes = entry.to_bytes();
        let decoded = DocIndexEntry::from_bytes(&bytes);
        assert_eq!(decoded.offset, 9876543210);
        assert_eq!(decoded.length, 1024);
    }

    #[test]
    fn test_block_header_roundtrip() {
        let header = BlockHeader {
            last_doc_id: 500,
            max_block_score: 3.14159,
            byte_len: 256,
        };
        let bytes = header.to_bytes();
        let decoded = BlockHeader::from_bytes(&bytes);
        assert_eq!(decoded.last_doc_id, 500);
        assert!((decoded.max_block_score - 3.14159).abs() < 1e-4);
        assert_eq!(decoded.byte_len, 256);
    }

    #[test]
    fn test_term_offset_entry_roundtrip() {
        let entry = TermOffsetEntry {
            offset: 12345,
            len: 10,
        };
        let bytes = entry.to_bytes();
        let decoded = TermOffsetEntry::from_bytes(&bytes);
        assert_eq!(decoded.offset, 12345);
        assert_eq!(decoded.len, 10);
    }

    #[test]
    fn test_block_size_constant() {
        assert_eq!(BLOCK_SIZE, 128);
    }

    #[test]
    fn test_entry_sizes() {
        assert_eq!(PostingsIndexEntry::SIZE, 12);
        assert_eq!(DocIndexEntry::SIZE, 12);
        assert_eq!(BlockHeader::SIZE, 10);
        assert_eq!(TermOffsetEntry::SIZE, 6);
    }
}
