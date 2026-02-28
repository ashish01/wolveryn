/// BM25 scoring and field length quantization.
///
/// BM25 formula:
///   score(D, Q) = Σ IDF(qi) × (tf × (k1 + 1)) / (tf + k1 × (1 - b + b × dl / avgdl))
///   IDF(qi) = ln((N - df + 0.5) / (df + 0.5) + 1)

/// BM25 parameters
pub const K1: f32 = 1.2;
pub const B: f32 = 0.75;

/// Compute IDF for a term.
/// - `doc_freq`: number of documents containing the term
/// - `total_docs`: total documents in the index
pub fn idf(doc_freq: u32, total_docs: u32) -> f32 {
    let n = total_docs as f64;
    let df = doc_freq as f64;
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln() as f32
}

/// Compute BM25 score for a single term in a single document.
/// - `tf`: term frequency in this document
/// - `dl`: document length (total tokens in document)
/// - `avgdl`: average document length across the collection
/// - `idf_val`: precomputed IDF for this term
pub fn bm25_term_score(tf: u32, dl: u32, avgdl: f32, idf_val: f32) -> f32 {
    let tf = tf as f32;
    let dl = dl as f32;
    let numerator = tf * (K1 + 1.0);
    let denominator = tf + K1 * (1.0 - B + B * dl / avgdl);
    idf_val * numerator / denominator
}

/// Compute the upper-bound BM25 score for a block.
/// Uses the maximum term frequency and minimum document length in the block
/// to get the tightest upper bound on the BM25 contribution.
/// - `max_tf`: maximum term frequency of any doc in the block
/// - `min_dl`: minimum document length of any doc in the block
/// - `avgdl`: average document length
/// - `idf_val`: precomputed IDF for this term
pub fn max_block_score(max_tf: u32, min_dl: u32, avgdl: f32, idf_val: f32) -> f32 {
    let tf = max_tf as f32;
    // Use min_dl to maximize the score (shorter doc → higher BM25 for same tf)
    let dl = (min_dl.max(1)) as f32;
    let numerator = tf * (K1 + 1.0);
    let denominator = tf + K1 * (1.0 - B + B * dl / avgdl);
    idf_val * numerator / denominator
}

// --- Field length quantization ---
// Encode document lengths into a single byte using a log-scale encoding.
// Inspired by Lucene's SmallFloat. Maps lengths 0..∞ to 0..255.
//
// Encoding: byte = floor(log2(dl)) * 16 + (dl >> (floor(log2(dl)) - 4)) & 0x0F
// Simplified approach: use a lookup table for decoding.

/// Field length quantization constant.
/// Factor controls precision vs range tradeoff.
/// 16 supports lengths up to ~65K with <5% error for typical doc lengths.
const NORM_FACTOR: f64 = 16.0;

/// Build the decode table once: byte → approximate field length.
fn decode_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    for i in 0..256 {
        let val = (2.0_f64.powf(i as f64 / NORM_FACTOR) - 1.0).round() as u32;
        table[i] = val.max(0);
    }
    table
}

/// Encode a field length to a single byte.
pub fn encode_field_length(len: u32) -> u8 {
    if len == 0 {
        return 0;
    }
    let encoded = ((len as f64 + 1.0).log2() * NORM_FACTOR) as u32;
    encoded.min(255) as u8
}

/// Decode a field length byte back to approximate length.
pub fn decode_field_length(byte: u8) -> u32 {
    let val = (2.0_f64.powf(byte as f64 / NORM_FACTOR) - 1.0).round() as u32;
    val.max(if byte > 0 { 1 } else { 0 })
}

/// Build a decode lookup table for batch use in scoring.
pub fn build_field_norm_table() -> [u32; 256] {
    let mut table = decode_table();
    table[0] = 0;
    for i in 1..256 {
        table[i] = table[i].max(1);
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idf_rare_term() {
        // Rare term: df=10 in N=10_000_000
        let val = idf(10, 10_000_000);
        assert!(val > 10.0, "rare term IDF should be high, got {}", val);
    }

    #[test]
    fn test_idf_common_term() {
        // Very common term: df=9_000_000 in N=10_000_000
        let val = idf(9_000_000, 10_000_000);
        assert!(val < 0.2, "common term IDF should be near zero, got {}", val);
    }

    #[test]
    fn test_idf_all_docs() {
        // Term in every doc: df=N
        let val = idf(10_000_000, 10_000_000);
        assert!(val >= 0.0, "IDF should never be negative, got {}", val);
        assert!(val < 0.1, "IDF for term in all docs should be near zero, got {}", val);
    }

    #[test]
    fn test_bm25_basic() {
        let idf_val = idf(1000, 10_000_000);
        let score = bm25_term_score(3, 100, 150.0, idf_val);
        assert!(score > 0.0);
    }

    #[test]
    fn test_bm25_higher_tf_higher_score() {
        let idf_val = idf(1000, 10_000_000);
        let score_low = bm25_term_score(1, 100, 150.0, idf_val);
        let score_high = bm25_term_score(5, 100, 150.0, idf_val);
        assert!(score_high > score_low, "higher tf should give higher score");
    }

    #[test]
    fn test_bm25_shorter_doc_higher_score() {
        let idf_val = idf(1000, 10_000_000);
        let score_short = bm25_term_score(2, 50, 150.0, idf_val);
        let score_long = bm25_term_score(2, 500, 150.0, idf_val);
        assert!(score_short > score_long, "shorter doc should score higher for same tf");
    }

    #[test]
    fn test_max_block_score_is_upper_bound() {
        let idf_val = idf(1000, 10_000_000);
        let avgdl = 150.0;

        // Block with max_tf=5, min_dl=20
        let upper = max_block_score(5, 20, avgdl, idf_val);

        // Any individual doc in this block should score <= upper
        for tf in 1..=5 {
            for dl in [20, 50, 100, 200, 500] {
                let score = bm25_term_score(tf, dl, avgdl, idf_val);
                assert!(
                    score <= upper + 1e-6,
                    "score({},{})={} > upper_bound={}",
                    tf, dl, score, upper
                );
            }
        }
    }

    #[test]
    fn test_field_length_quantization_roundtrip() {
        let test_lengths = [0, 1, 2, 5, 10, 50, 100, 500, 1000, 5000, 10000];
        for &len in &test_lengths {
            let encoded = encode_field_length(len);
            let decoded = decode_field_length(encoded);
            if len == 0 {
                assert_eq!(decoded, 0);
            } else {
                // Within ~10% for small values, wider for large values
                let error = (decoded as f64 - len as f64).abs() / len as f64;
                assert!(
                    error < 0.15 || (len <= 2 && (decoded as i64 - len as i64).abs() <= 1),
                    "length {} encoded to {} decoded to {}, error {:.1}%",
                    len, encoded, decoded, error * 100.0
                );
            }
        }
    }

    #[test]
    fn test_field_length_monotonic() {
        // Larger lengths should encode to >= byte values
        let mut prev_byte = 0u8;
        for len in [1, 10, 100, 1000, 10000] {
            let byte = encode_field_length(len);
            assert!(byte >= prev_byte, "encoding should be monotonic");
            prev_byte = byte;
        }
    }

    #[test]
    fn test_field_norm_table() {
        let table = build_field_norm_table();
        assert_eq!(table[0], 0);
        for i in 1..256 {
            assert!(table[i] >= 1, "non-zero byte should decode to >= 1");
        }
        // Table should be monotonically non-decreasing
        for i in 1..256 {
            assert!(table[i] >= table[i - 1], "table should be monotonic");
        }
    }
}
