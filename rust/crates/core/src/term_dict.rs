/// Term dictionary reader: memory-mapped sorted term table with binary search.
///
/// File format (terms.bin):
/// [term_count: u32]
/// [offset_table: term_count × TermOffsetEntry (6 bytes each)]
/// [term_bytes: concatenated UTF-8 term strings]

use crate::segment::TermOffsetEntry;
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

/// A memory-mapped term dictionary supporting O(log n) lookups.
pub struct TermDict {
    mmap: Mmap,
    term_count: u32,
    /// Byte offset where the offset table starts (always 4).
    offset_table_start: usize,
    /// Byte offset where the concatenated term bytes start.
    term_bytes_start: usize,
}

impl TermDict {
    /// Open a term dictionary from a terms.bin file.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let term_count = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        let offset_table_start = 4;
        let term_bytes_start = offset_table_start + (term_count as usize) * TermOffsetEntry::SIZE;

        Ok(Self {
            mmap,
            term_count,
            offset_table_start,
            term_bytes_start,
        })
    }

    /// Number of terms in the dictionary.
    pub fn term_count(&self) -> u32 {
        self.term_count
    }

    /// Look up a term by string. Returns the term_id if found.
    pub fn lookup(&self, term: &str) -> Option<u32> {
        let target = term.as_bytes();
        let mut lo: u32 = 0;
        let mut hi: u32 = self.term_count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_term = self.get_term_bytes(mid);
            match mid_term.cmp(target) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// Get the term string for a given term_id.
    pub fn get_term(&self, term_id: u32) -> &str {
        let bytes = self.get_term_bytes(term_id);
        std::str::from_utf8(bytes).expect("term should be valid UTF-8")
    }

    /// Get raw bytes for a term_id.
    fn get_term_bytes(&self, term_id: u32) -> &[u8] {
        let entry_offset = self.offset_table_start + (term_id as usize) * TermOffsetEntry::SIZE;
        let entry = TermOffsetEntry::from_bytes(&self.mmap[entry_offset..]);
        let start = self.term_bytes_start + entry.offset as usize;
        let end = start + entry.len as usize;
        &self.mmap[start..end]
    }

    /// Iterate all terms in order. Returns (term_id, term_string) pairs.
    pub fn iter(&self) -> TermDictIter<'_> {
        TermDictIter {
            dict: self,
            pos: 0,
        }
    }
}

pub struct TermDictIter<'a> {
    dict: &'a TermDict,
    pos: u32,
}

impl<'a> Iterator for TermDictIter<'a> {
    type Item = (u32, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.dict.term_count {
            return None;
        }
        let id = self.pos;
        let term = self.dict.get_term(id);
        self.pos += 1;
        Some((id, term))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_writer::IndexWriter;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("search_test_td_{}_{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn create_test_segment(name: &str) -> PathBuf {
        let dir = test_dir(name);
        let seg_dir = dir.join("segment_0");

        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({"body": "zebra apple mango banana cherry"}));
        writer.add_document(&json!({"body": "apple banana date"}));
        writer.add_document(&json!({"body": "cherry date elderberry fig"}));
        writer.flush_segment(&seg_dir).unwrap();

        seg_dir
    }

    #[test]
    fn test_lookup_existing_term() {
        let seg = create_test_segment("lookup_existing");
        let dict = TermDict::open(&seg.join("terms.bin")).unwrap();

        assert!(dict.lookup("apple").is_some());
        assert!(dict.lookup("banana").is_some());
        assert!(dict.lookup("zebra").is_some());

        let _ = fs::remove_dir_all(seg.parent().unwrap());
    }

    #[test]
    fn test_lookup_nonexistent_term() {
        let seg = create_test_segment("lookup_nonexistent");
        let dict = TermDict::open(&seg.join("terms.bin")).unwrap();

        assert!(dict.lookup("nothere").is_none());
        assert!(dict.lookup("zzz").is_none());
        assert!(dict.lookup("aaa").is_none());

        let _ = fs::remove_dir_all(seg.parent().unwrap());
    }

    #[test]
    fn test_term_count() {
        let seg = create_test_segment("term_count");
        let dict = TermDict::open(&seg.join("terms.bin")).unwrap();

        // Terms: apple, banana, cherry, date, elderberry, fig, mango, zebra = 8
        assert_eq!(dict.term_count(), 8);

        let _ = fs::remove_dir_all(seg.parent().unwrap());
    }

    #[test]
    fn test_get_term() {
        let seg = create_test_segment("get_term");
        let dict = TermDict::open(&seg.join("terms.bin")).unwrap();

        // First term alphabetically should be "apple"
        assert_eq!(dict.get_term(0), "apple");

        let _ = fs::remove_dir_all(seg.parent().unwrap());
    }

    #[test]
    fn test_binary_search_boundaries() {
        let seg = create_test_segment("boundaries");
        let dict = TermDict::open(&seg.join("terms.bin")).unwrap();

        // First term
        let first_id = dict.lookup("apple").unwrap();
        assert_eq!(dict.get_term(first_id), "apple");

        // Last term
        let last_id = dict.lookup("zebra").unwrap();
        assert_eq!(dict.get_term(last_id), "zebra");

        // Middle term
        let mid_id = dict.lookup("date").unwrap();
        assert_eq!(dict.get_term(mid_id), "date");

        let _ = fs::remove_dir_all(seg.parent().unwrap());
    }

    #[test]
    fn test_iteration() {
        let seg = create_test_segment("iteration");
        let dict = TermDict::open(&seg.join("terms.bin")).unwrap();

        let terms: Vec<String> = dict.iter().map(|(_, t)| t.to_string()).collect();
        assert_eq!(terms.len(), 8);
        // Verify sorted
        for i in 1..terms.len() {
            assert!(terms[i] > terms[i - 1], "terms should be sorted");
        }

        let _ = fs::remove_dir_all(seg.parent().unwrap());
    }
}
