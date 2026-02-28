/// Variable-byte (VByte) encoding for u32 values.
///
/// Uses 7 data bits per byte. The MSB (high bit) is the continuation bit:
/// - 0 = more bytes follow
/// - 1 = this is the last byte

/// Encode a single u32 value, appending bytes to `buf`.
/// Returns the number of bytes written (1-5).
pub fn encode_u32(mut val: u32, buf: &mut Vec<u8>) -> usize {
    let start = buf.len();
    loop {
        let mut byte = (val & 0x7F) as u8; // low 7 bits
        val >>= 7;
        if val == 0 {
            byte |= 0x80; // set high bit = last byte
            buf.push(byte);
            break;
        }
        buf.push(byte); // high bit clear = more bytes follow
    }
    buf.len() - start
}

/// Decode a single u32 from a byte slice.
/// Returns `(value, bytes_consumed)`.
///
/// # Panics
/// Panics if the slice does not contain a complete vbyte-encoded value.
pub fn decode_u32(data: &[u8]) -> (u32, usize) {
    let mut val: u32 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        val |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 != 0 {
            return (val, i + 1);
        }
        shift += 7;
        if shift > 28 {
            // Safety: u32 max needs at most 5 bytes (5*7=35 > 32)
            // After 5 bytes without terminator, data is malformed
            if i >= 4 {
                panic!("malformed vbyte: more than 5 bytes for u32");
            }
        }
    }
    panic!("unexpected end of vbyte data");
}

/// Encode a slice of u32 values with delta encoding.
/// Stores gaps between consecutive values (first value stored as-is).
/// Input must be sorted in ascending order.
pub fn encode_delta(values: &[u32], buf: &mut Vec<u8>) {
    let mut prev: u32 = 0;
    for &val in values {
        debug_assert!(val >= prev, "delta encode requires sorted input");
        let gap = val - prev;
        encode_u32(gap, buf);
        prev = val;
    }
}

/// Decode `count` delta-encoded u32 values from a byte slice.
/// Returns the decoded values and total bytes consumed.
pub fn decode_delta(data: &[u8], count: usize) -> (Vec<u32>, usize) {
    let mut values = Vec::with_capacity(count);
    let mut offset = 0;
    let mut prev: u32 = 0;
    for _ in 0..count {
        let (gap, consumed) = decode_u32(&data[offset..]);
        prev += gap;
        values.push(prev);
        offset += consumed;
    }
    (values, offset)
}

/// Encode interleaved (doc_id_gap, term_freq) pairs.
/// `postings` must be sorted by doc_id.
/// Format: [gap0, tf0, gap1, tf1, ...]
pub fn encode_postings(postings: &[(u32, u32)], buf: &mut Vec<u8>) {
    let mut prev_doc: u32 = 0;
    for &(doc_id, tf) in postings {
        debug_assert!(doc_id >= prev_doc, "postings must be sorted by doc_id");
        let gap = doc_id - prev_doc;
        encode_u32(gap, buf);
        encode_u32(tf, buf);
        prev_doc = doc_id;
    }
}

/// Decode interleaved (doc_id, term_freq) pairs from vbyte data.
/// Returns decoded postings and bytes consumed.
/// `base_doc_id` is added to the first decoded gap (for block-based decoding).
pub fn decode_postings(data: &[u8], count: usize, base_doc_id: u32) -> (Vec<(u32, u32)>, usize) {
    let mut result = Vec::with_capacity(count);
    let mut offset = 0;
    let mut prev_doc = base_doc_id;
    for _ in 0..count {
        let (gap, c1) = decode_u32(&data[offset..]);
        offset += c1;
        let (tf, c2) = decode_u32(&data[offset..]);
        offset += c2;
        prev_doc += gap;
        result.push((prev_doc, tf));
    }
    (result, offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_small() {
        for val in [0u32, 1, 63, 127] {
            let mut buf = Vec::new();
            encode_u32(val, &mut buf);
            assert_eq!(buf.len(), 1, "value {} should be 1 byte", val);
            let (decoded, consumed) = decode_u32(&buf);
            assert_eq!(decoded, val);
            assert_eq!(consumed, 1);
        }
    }

    #[test]
    fn test_encode_decode_medium() {
        for val in [128u32, 255, 16383] {
            let mut buf = Vec::new();
            encode_u32(val, &mut buf);
            assert_eq!(buf.len(), 2, "value {} should be 2 bytes", val);
            let (decoded, consumed) = decode_u32(&buf);
            assert_eq!(decoded, val);
            assert_eq!(consumed, 2);
        }
    }

    #[test]
    fn test_encode_decode_large() {
        let val = 16384u32;
        let mut buf = Vec::new();
        encode_u32(val, &mut buf);
        assert_eq!(buf.len(), 3);
        let (decoded, consumed) = decode_u32(&buf);
        assert_eq!(decoded, val);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_encode_decode_u32_max() {
        let val = u32::MAX;
        let mut buf = Vec::new();
        encode_u32(val, &mut buf);
        assert!(buf.len() <= 5);
        let (decoded, consumed) = decode_u32(&buf);
        assert_eq!(decoded, val);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_multiple_values() {
        let values = [0u32, 1, 127, 128, 16383, 16384, u32::MAX];
        let mut buf = Vec::new();
        for &v in &values {
            encode_u32(v, &mut buf);
        }
        let mut offset = 0;
        for &expected in &values {
            let (decoded, consumed) = decode_u32(&buf[offset..]);
            assert_eq!(decoded, expected);
            offset += consumed;
        }
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn test_delta_encode_decode() {
        let values = [100u32, 105, 200, 201, 500];
        let mut buf = Vec::new();
        encode_delta(&values, &mut buf);
        let (decoded, consumed) = decode_delta(&buf, values.len());
        assert_eq!(decoded, values);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_postings_encode_decode() {
        let postings = [(100u32, 3u32), (105, 1), (200, 5), (201, 2)];
        let mut buf = Vec::new();
        encode_postings(&postings, &mut buf);
        let (decoded, consumed) = decode_postings(&buf, postings.len(), 0);
        assert_eq!(decoded, postings);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_postings_with_base_doc_id() {
        // Simulate block-based decoding: block starts at doc_id 1000
        let postings = [(5u32, 2u32), (10, 1), (15, 3)]; // gaps relative to 0
        let mut buf = Vec::new();
        encode_postings(&postings, &mut buf);
        // Decode with base 1000
        let (decoded, _) = decode_postings(&buf, postings.len(), 1000);
        assert_eq!(decoded, [(1005, 2), (1010, 1), (1015, 3)]);
    }

    #[test]
    fn test_round_trip_all_byte_sizes() {
        // Test values at each byte boundary
        let test_values: Vec<u32> = vec![
            0, 1, 126, 127,           // 1 byte
            128, 16383,                 // 2 bytes
            16384, 2097151,             // 3 bytes
            2097152, 268435455,         // 4 bytes
            268435456, u32::MAX,        // 5 bytes
        ];
        for val in test_values {
            let mut buf = Vec::new();
            encode_u32(val, &mut buf);
            let (decoded, _) = decode_u32(&buf);
            assert_eq!(decoded, val, "round-trip failed for {}", val);
        }
    }
}
