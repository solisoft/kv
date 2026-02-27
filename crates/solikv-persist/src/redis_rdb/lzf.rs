use std::io;

/// Decompress LZF-compressed data.
pub fn decompress(compressed: &[u8], expected_len: usize) -> io::Result<Vec<u8>> {
    lzf::decompress(compressed, expected_len).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("LZF decompression failed: {e}"),
        )
    })
}
