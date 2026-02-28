/// Segment merger: merges N segments into 1.
///
/// Iterates all term dictionaries in parallel (merge-sort),
/// re-encodes postings with new contiguous doc_ids,
/// recomputes block-max scores, and merges doc stores.

use crate::postings::{PostingIterator, TermPostingIterator};
use crate::scorer;
use crate::segment::*;
use crate::term_dict::TermDict;
use crate::vbyte;
use memmap2::Mmap;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;

/// Merge multiple segments into a single segment.
///
/// - `segment_dirs`: paths to existing segment directories
/// - `output_dir`: path for the merged segment
///
/// Returns the merged segment's metadata.
pub fn merge_segments(segment_dirs: &[&Path], output_dir: &Path) -> std::io::Result<SegmentMeta> {
    fs::create_dir_all(output_dir)?;

    // Open all source segments
    let mut segments: Vec<SourceSegment> = Vec::new();
    let mut total_docs: u32 = 0;

    for dir in segment_dirs {
        let seg = SourceSegment::open(dir)?;
        total_docs += seg.meta.doc_count;
        segments.push(seg);
    }

    // Compute doc_id remapping: each segment gets a contiguous range
    let mut doc_id_bases: Vec<u32> = Vec::new();
    let mut base: u32 = 0;
    for seg in &segments {
        doc_id_bases.push(base);
        base += seg.meta.doc_count;
    }

    // Merge all terms and their postings
    // Collect all unique terms across all segments
    let mut all_terms: BTreeMap<String, Vec<(usize, u32)>> = BTreeMap::new(); // term → [(seg_idx, term_id)]
    for (seg_idx, seg) in segments.iter().enumerate() {
        for (term_id, term) in seg.term_dict.iter() {
            all_terms
                .entry(term.to_string())
                .or_default()
                .push((seg_idx, term_id));
        }
    }

    // Compute total tokens and avgdl for the merged segment
    let total_tokens: u64 = segments.iter().map(|s| s.meta.total_tokens).sum();
    let avgdl = if total_docs > 0 {
        total_tokens as f32 / total_docs as f32
    } else {
        1.0
    };

    let files = segment_files(output_dir);

    // Write merged fieldnorms
    let mut all_norms: Vec<u8> = Vec::new();
    for seg in &segments {
        all_norms.extend_from_slice(&seg.fieldnorm_bytes);
    }
    fs::write(&files.fieldnorms, &all_norms)?;

    // Write merged docs.bin and docs_index.bin
    let mut docs_file = fs::File::create(&files.docs)?;
    let mut docs_index_file = fs::File::create(&files.docs_index)?;
    let mut doc_offset: u64 = 0;
    for seg in &segments {
        for local_id in 0..seg.meta.doc_count {
            let idx_off = local_id as usize * DocIndexEntry::SIZE;
            let entry = DocIndexEntry::from_bytes(&seg.docs_index_mmap[idx_off..]);
            let start = entry.offset as usize;
            let end = start + entry.length as usize;
            let doc_bytes = &seg.docs_mmap[start..end];

            let new_entry = DocIndexEntry {
                offset: doc_offset,
                length: entry.length,
            };
            docs_index_file.write_all(&new_entry.to_bytes())?;
            docs_file.write_all(doc_bytes)?;
            doc_offset += entry.length as u64;
        }
    }

    // Build norm decode table for block-max computation
    let norm_table = scorer::build_field_norm_table();

    // Write merged terms.bin, postings.bin, postings_index.bin
    
    let mut postings_file = fs::File::create(&files.postings)?;
    let mut postings_index_file = fs::File::create(&files.postings_index)?;
    let mut postings_offset: u64 = 0;

    let term_count = all_terms.len() as u32;

    // Build term dictionary data
    let mut term_offsets: Vec<TermOffsetEntry> = Vec::new();
    let mut term_bytes_buf: Vec<u8> = Vec::new();

    for (term, sources) in &all_terms {
        // Collect all postings for this term across segments, with remapped doc_ids
        let mut merged_postings: Vec<(u32, u32)> = Vec::new(); // (new_doc_id, tf)
        let mut total_df: u32 = 0;

        for &(seg_idx, term_id) in sources {
            let seg = &segments[seg_idx];
            let pi_off = term_id as usize * PostingsIndexEntry::SIZE;
            let entry = PostingsIndexEntry::from_bytes(&seg.postings_index_mmap[pi_off..]);
            total_df += entry.doc_freq;

            let mut iter = TermPostingIterator::new(&seg.postings_mmap, &entry);
            let base = doc_id_bases[seg_idx];
            let seg_base = seg.meta.min_doc_id;

            while let Some(doc_id) = iter.next_doc() {
                let new_doc_id = base + (doc_id - seg_base);
                merged_postings.push((new_doc_id, iter.term_freq()));
            }
        }

        merged_postings.sort_by_key(|&(doc_id, _)| doc_id);

        // Compute IDF for block-max scores
        let idf_val = scorer::idf(total_df, total_docs);

        // Write postings index entry
        let idx_entry = PostingsIndexEntry {
            offset: postings_offset,
            doc_freq: merged_postings.len() as u32,
        };
        postings_index_file.write_all(&idx_entry.to_bytes())?;

        // Write block count
        let num_blocks = (merged_postings.len() + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let block_count_bytes = (num_blocks as u32).to_le_bytes();
        postings_file.write_all(&block_count_bytes)?;
        postings_offset += 4;

        // Write blocks
        for block_idx in 0..num_blocks {
            let start = block_idx * BLOCK_SIZE;
            let end = (start + BLOCK_SIZE).min(merged_postings.len());
            let block = &merged_postings[start..end];

            // Compute block-max score
            let mut max_tf: u32 = 0;
            let mut min_dl: u32 = u32::MAX;
            for &(doc_id, tf) in block {
                max_tf = max_tf.max(tf);
                let norm_byte = all_norms[doc_id as usize];
                let dl = norm_table[norm_byte as usize].max(1);
                min_dl = min_dl.min(dl);
            }
            let block_max = scorer::max_block_score(max_tf, min_dl, avgdl, idf_val);

            // Delta-encode postings within block
            let base_doc = if block_idx == 0 {
                0u32
            } else {
                merged_postings[start - 1].0
            };
            let relative: Vec<(u32, u32)> = block
                .iter()
                .map(|&(doc_id, tf)| (doc_id - base_doc, tf))
                .collect();

            let mut compressed = Vec::new();
            vbyte::encode_postings(&relative, &mut compressed);

            let header = BlockHeader {
                last_doc_id: block.last().unwrap().0,
                max_block_score: block_max,
                byte_len: compressed.len() as u16,
            };

            postings_file.write_all(&header.to_bytes())?;
            postings_file.write_all(&compressed)?;
            postings_offset += BlockHeader::SIZE as u64 + compressed.len() as u64;
        }

        // Add to term dictionary
        let term_b = term.as_bytes();
        let entry = TermOffsetEntry {
            offset: term_bytes_buf.len() as u32,
            len: term_b.len() as u16,
        };
        term_offsets.push(entry);
        term_bytes_buf.extend_from_slice(term_b);
    }

    // Write terms.bin
    let mut terms_file = fs::File::create(&files.terms)?;
    terms_file.write_all(&term_count.to_le_bytes())?;
    for entry in &term_offsets {
        terms_file.write_all(&entry.to_bytes())?;
    }
    terms_file.write_all(&term_bytes_buf)?;

    // Write meta.json
    let fields = if !segments.is_empty() {
        segments[0].meta.fields.clone()
    } else {
        Vec::new()
    };

    let meta = SegmentMeta {
        doc_count: total_docs,
        total_tokens,
        avgdl,
        min_doc_id: 0,
        max_doc_id: if total_docs > 0 { total_docs - 1 } else { 0 },
        term_count,
        fields,
    };
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(&files.meta, meta_json)?;

    Ok(meta)
}

/// A source segment opened for reading during merge.
struct SourceSegment {
    meta: SegmentMeta,
    term_dict: TermDict,
    postings_mmap: Mmap,
    postings_index_mmap: Mmap,
    fieldnorm_bytes: Vec<u8>,
    docs_mmap: Mmap,
    docs_index_mmap: Mmap,
}

impl SourceSegment {
    fn open(dir: &Path) -> std::io::Result<Self> {
        let files = segment_files(dir);

        let meta_str = fs::read_to_string(&files.meta)?;
        let meta: SegmentMeta = serde_json::from_str(&meta_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let term_dict = TermDict::open(&files.terms)?;

        let pf = fs::File::open(&files.postings)?;
        let postings_mmap = unsafe { Mmap::map(&pf)? };

        let pif = fs::File::open(&files.postings_index)?;
        let postings_index_mmap = unsafe { Mmap::map(&pif)? };

        let fieldnorm_bytes = fs::read(&files.fieldnorms)?;

        let df = fs::File::open(&files.docs)?;
        let docs_mmap = unsafe { Mmap::map(&df)? };

        let dif = fs::File::open(&files.docs_index)?;
        let docs_index_mmap = unsafe { Mmap::map(&dif)? };

        Ok(Self {
            meta,
            term_dict,
            postings_mmap,
            postings_index_mmap,
            fieldnorm_bytes,
            docs_mmap,
            docs_index_mmap,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_writer::IndexWriter;
    use crate::searcher::Searcher;
    use serde_json::json;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "search_test_merger_{}_{}",
            std::process::id(),
            name
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_merge_two_segments() {
        let dir = test_dir("merge_two");

        // Create segment 0
        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({"body": "rust programming"}));
        writer.add_document(&json!({"body": "rust web development"}));
        writer.flush_segment(&dir.join("segment_0")).unwrap();

        // Create segment 1
        writer.add_document(&json!({"body": "rust systems"}));
        writer.add_document(&json!({"body": "python scripting"}));
        writer.flush_segment(&dir.join("segment_1")).unwrap();

        // Merge
        let merged_dir = dir.join("merged_0");
        let meta = merge_segments(
            &[&dir.join("segment_0"), &dir.join("segment_1")],
            &merged_dir,
        )
        .unwrap();

        assert_eq!(meta.doc_count, 4);
        assert_eq!(meta.min_doc_id, 0);
        assert_eq!(meta.max_doc_id, 3);

        // Verify search works on merged segment
        // Create a temp index dir with just the merged segment
        let search_dir = dir.join("search_index");
        fs::create_dir_all(&search_dir).unwrap();
        // Rename merged to segment_0 in search dir
        fs::rename(&merged_dir, search_dir.join("segment_0")).unwrap();

        let searcher = Searcher::open(&search_dir).unwrap();
        let results = searcher.search("rust", 10, 0).unwrap();
        assert_eq!(results.total_hits, 3); // docs 0, 1, 2

        let results2 = searcher.search("python", 10, 0).unwrap();
        assert_eq!(results2.total_hits, 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_three_segments() {
        let dir = test_dir("merge_three");

        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({"body": "alpha one"}));
        writer.flush_segment(&dir.join("segment_0")).unwrap();

        writer.add_document(&json!({"body": "beta two"}));
        writer.flush_segment(&dir.join("segment_1")).unwrap();

        writer.add_document(&json!({"body": "gamma three"}));
        writer.flush_segment(&dir.join("segment_2")).unwrap();

        let merged_dir = dir.join("merged_0");
        let meta = merge_segments(
            &[
                &dir.join("segment_0"),
                &dir.join("segment_1"),
                &dir.join("segment_2"),
            ],
            &merged_dir,
        )
        .unwrap();

        assert_eq!(meta.doc_count, 3);
        assert_eq!(meta.term_count, 6); // alpha, beta, gamma, one, two, three

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_preserves_documents() {
        let dir = test_dir("merge_docs");

        let mut writer = IndexWriter::new(vec!["body".into()], 0);
        writer.add_document(&json!({"body": "hello world"}));
        writer.flush_segment(&dir.join("segment_0")).unwrap();

        writer.add_document(&json!({"body": "foo bar"}));
        writer.flush_segment(&dir.join("segment_1")).unwrap();

        let merged_dir = dir.join("merged_0");
        merge_segments(
            &[&dir.join("segment_0"), &dir.join("segment_1")],
            &merged_dir,
        )
        .unwrap();

        let search_dir = dir.join("search_index");
        fs::create_dir_all(&search_dir).unwrap();
        fs::rename(&merged_dir, search_dir.join("segment_0")).unwrap();

        let searcher = Searcher::open(&search_dir).unwrap();
        let results = searcher.search("hello", 10, 0).unwrap();
        assert_eq!(results.hits.len(), 1);
        assert_eq!(results.hits[0].doc["body"], "hello world");

        let _ = fs::remove_dir_all(&dir);
    }
}
