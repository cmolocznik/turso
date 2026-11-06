//! CRC-32C (Castagnoli), the checksum of log records, pages and the control file.
//!
//! The `crc32c` crate computes the same values, but unless the whole build enables SSE 4.2 its
//! hardware path calls a function per 8 bytes, which the compiler cannot inline into a caller
//! without the feature: measured on the log's commit, about 9 instructions per 8 bytes. Here the
//! whole loop is compiled with the feature, picked once SSE 4.2 is detected, and long inputs run
//! three lanes at once, since the instruction's latency is three times its throughput. Elsewhere
//! the crate's own code runs.

/// The CRC of `data` continuing from `crc`, the CRC of what came before it (0 for nothing), as
/// `crc32c::crc32c_append` gives it.
#[inline]
pub fn append(crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("sse4.2") {
        // SAFETY: the feature was just detected.
        return unsafe { x86::append(crc, data) };
    }
    crc32c::crc32c_append(crc, data)
}

/// The CRC of `data`.
#[inline]
pub fn of(data: &[u8]) -> u32 {
    append(0, data)
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};
    use std::sync::OnceLock;

    /// Bytes of each of the three lanes a long input runs in. Joining the lanes costs two shifts
    /// of the register, eight table reads, per three lanes: at 512 bytes about 1% of the lanes'
    /// own time, and an input shorter than three lanes takes the one lane, at a third of the
    /// speed, on less than 1.5 KiB.
    const LANE: usize = 512;

    /// The register, as the instruction keeps it, after `LANE` zero bytes more: by a table per
    /// byte of the register, since the shift is linear in it.
    fn shift(r: u32) -> u32 {
        let t = TABLES.get_or_init(tables);
        t[0][(r & 0xff) as usize]
            ^ t[1][((r >> 8) & 0xff) as usize]
            ^ t[2][((r >> 16) & 0xff) as usize]
            ^ t[3][(r >> 24) as usize]
    }

    static TABLES: OnceLock<[[u32; 256]; 4]> = OnceLock::new();

    fn tables() -> [[u32; 256]; 4] {
        let mut t = [[0; 256]; 4];
        for (k, table) in t.iter_mut().enumerate() {
            for (b, entry) in table.iter_mut().enumerate() {
                let mut r = (b as u64) << (8 * k);
                for _ in 0..LANE / 8 {
                    // SAFETY: the tables are built only once SSE 4.2 is detected.
                    r = unsafe { zeros(r) };
                }
                *entry = r as u32;
            }
        }
        t
    }

    #[target_feature(enable = "sse4.2")]
    unsafe fn zeros(r: u64) -> u64 {
        _mm_crc32_u64(r, 0)
    }

    #[inline]
    fn word(data: &[u8], at: usize) -> u64 {
        u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
    }

    /// # Safety
    /// SSE 4.2 must be available.
    #[target_feature(enable = "sse4.2")]
    pub unsafe fn append(crc: u32, data: &[u8]) -> u32 {
        let mut r = u64::from(!crc);
        let mut d = data;
        while d.len() >= 3 * LANE {
            let (mut a, mut b, mut c) = (r, 0u64, 0u64);
            let mut at = 0;
            while at < LANE {
                a = _mm_crc32_u64(a, word(d, at));
                b = _mm_crc32_u64(b, word(d, LANE + at));
                c = _mm_crc32_u64(c, word(d, 2 * LANE + at));
                at += 8;
            }
            r = u64::from(shift(shift(a as u32) ^ b as u32) ^ c as u32);
            d = &d[3 * LANE..];
        }
        let mut at = 0;
        while at + 32 <= d.len() {
            r = _mm_crc32_u64(r, word(d, at));
            r = _mm_crc32_u64(r, word(d, at + 8));
            r = _mm_crc32_u64(r, word(d, at + 16));
            r = _mm_crc32_u64(r, word(d, at + 24));
            at += 32;
        }
        while at + 8 <= d.len() {
            r = _mm_crc32_u64(r, word(d, at));
            at += 8;
        }
        let mut r = r as u32;
        for &byte in &d[at..] {
            r = _mm_crc32_u8(r, byte);
        }
        !r
    }
}

#[cfg(test)]
mod tests {
    /// Every length up to past four lane triples, at every alignment within a word, from a seed
    /// and from none, gives the crate's value.
    #[test]
    fn it_agrees_with_the_crate() {
        let data: Vec<u8> = (0..8000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        let mut checked = 0;
        for offset in 0..8 {
            for len in (0..6300).step_by(7).chain(1530..1550).chain(3070..3080) {
                let d = &data[offset..offset + len];
                for seed in [0, 0xdead_beef] {
                    assert_eq!(
                        super::append(seed, d),
                        crc32c::crc32c_append(seed, d),
                        "offset {offset} length {len} seed {seed:#x}"
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 10_000, "{checked} cases");
        assert_eq!(super::of(b"123456789"), 0xe306_9283);
    }
}
