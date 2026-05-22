/// An RLike version of the Apache Arrow Row implementation for calculating DataFrame
/// keys to support row-to-row comparisons for sorting, joining, grouping and indexing.
/// 
/// See: https://arrow.apache.org/blog/2022/11/07/multi-column-sorts-in-arrow-rust-part-2/

// dependencies
use std::mem::transmute_copy;

// memory encoding of Option<T> for Rust primitive types
// size_of bool:   1, Option<bool>:   1   == one byte with Some/None in second bit (see below)
// size_of u16:    2, Option<u16>:    4   == Some/None byte, 1 padding bytes, 2 value bytes
// size_of i32:    4, Option<i32>:    8   == Some/None byte, 3 padding bytes, 4 value bytes
// size_of f64:    8, Option<f64>:    16  == Some/None byte, 7 padding bytes, 8 value bytes
// size_of usize:  8, Option<usize>:  16  == Some/None byte, 7 padding bytes, 8 value bytes

/// A CellKey is a 9-byte array that can be compared lexicographically to order and equate values.
/// The first byte is a flag indicating whether the value is None (0) or Some (1). The remaining 
/// 8 bytes are the packed value, with the sign bit flipped for signed integers and floating point 
/// numbers. In being stored as [u8], Vec<CellKey> remains 1-byte aligned in memory.
pub type CellKey = [u8; 9];

/// NA_CELL_KEY is a 9-byte array that represents None, i.e., NA value in a CellKey.
pub const NA_CELL_KEY: [u8; 9] = [0_u8; 9];

/// Trait CellKeyValue is implemented for RLike Option<T> types that can be packed into a CellKey 
/// using the pack method. Notably, RString columns cannot be packed into a CellKey.
pub trait CellKeyValue {
    fn pack(&self, negate: bool) -> CellKey;
}
impl CellKeyValue for Option<i32> { // desired sort: None, -2^31, -1, 0, 1, 2^31-1
    fn pack(&self, negate: bool) -> CellKey {
        let bytes = unsafe { transmute_copy::<_, [u8; 8]>(self) };
        if bytes[0] == 0 {
            NA_CELL_KEY
        } else { // flip the sign bit
            if negate {
                [1_u8, 0xFF, 0xFF, 0xFF, 0xFF, (bytes[7] ^ 0x80) ^ 0xFF, bytes[6] ^ 0xFF, bytes[5] ^ 0xFF, bytes[4] ^ 0xFF]
            } else {
                [1_u8, 0_u8, 0_u8, 0_u8, 0_u8, bytes[7] ^ 0x80, bytes[6], bytes[5], bytes[4]]
            }
        }
    }
}
impl CellKeyValue for Option<f64> { // desired sort: None, -inf, -1, 0, 1, inf, NaN
    fn pack(&self, negate: bool) -> CellKey {
        let bytes = unsafe { transmute_copy::<_, [u8; 16]>(self) };
        if bytes[0] == 0 {
            NA_CELL_KEY
        } else { // see f64::total_cmp also
            let value = f64::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]]);
            let mut as_i64 = value.to_bits() as i64;
            as_i64 ^= (((as_i64 >> 63) as u64) >> 1) as i64;
            let bytes = as_i64.to_le_bytes();
            if negate {
                [1_u8, (bytes[7] ^ 0x80) ^ 0xFF, bytes[6] ^ 0xFF, bytes[5] ^ 0xFF, bytes[4] ^ 0xFF, bytes[3] ^ 0xFF, bytes[2] ^ 0xFF, bytes[1] ^ 0xFF, bytes[0] ^ 0xFF]
            } else {
                [1_u8, bytes[7] ^ 0x80, bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]]
            }
        }
    }
}
impl CellKeyValue for Option<bool> { // desired sort: None, false, true
    fn pack(&self, negate: bool) -> CellKey {
        let byte = unsafe { transmute_copy::<_, u8>(self) }; // one byte
        if byte & 0x2 == 0x2 { // None has bit 2 == 1
            NA_CELL_KEY
        } else {
            if negate {
                [1_u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, (byte & 0x1) ^ 0xFF]
            } else {
                [1_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, byte & 0x1]
            }
        }
    }
}
impl CellKeyValue for Option<u16> { // desired sort: None, 0, 1, 2^16-1
    fn pack(&self, negate: bool) -> CellKey {
        let bytes = unsafe { transmute_copy::<_, [u8; 4]>(self) };
        if bytes[0] == 0 {
            NA_CELL_KEY
        } else {
            if negate {
                [1_u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, bytes[3] ^ 0xFF, bytes[2] ^ 0xFF]
            } else {
                [1_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, bytes[3], bytes[2]]
            }
        }
    }
}
impl CellKeyValue for Option<usize> { // desired sort: None, 0, 1, 2^64-1
    fn pack(&self, negate: bool) -> CellKey {
        let bytes = unsafe { transmute_copy::<_, [u8; 16]>(self) };
        if bytes[0] == 0 {
            NA_CELL_KEY
        } else {
            if negate {
                [1_u8, bytes[15] ^ 0xFF, bytes[14] ^ 0xFF, bytes[13] ^ 0xFF, bytes[12] ^ 0xFF, bytes[11] ^ 0xFF, bytes[10] ^ 0xFF, bytes[9] ^ 0xFF, bytes[8] ^ 0xFF]
            } else {
                [1_u8, bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]]
            }
            
        }
    }
}

/// Trait RowKey is implemented for RowKey2, RowKey4 and RowKey8, which collapse 2, 3..=4, and
/// 5..=8 CellKeys into a single row key, respectively. 
/// 
/// The first CellKey in the array is the major key.
/// 
/// This strategy creates a balance between code complexity and memory efficiency.
pub trait RowKey {}
macro_rules! impl_row_key {
    ($name:ident, $n_bytes:expr) => {
        /// A RowKey is a fixed-size array of CellKeys. The first CellKey in the array is the major key.
        /// Because a CellKey always uses 9 bytes, any Option<T> data type can be placed into any key position.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name ([u8; $n_bytes]);
        impl $name {
            pub fn init_vec(n_row: usize) -> Vec<[u8; $n_bytes]> {
                vec!([0_u8; $n_bytes]; n_row)
            }
        }
        impl RowKey for $name {}
    };
}
impl_row_key!(RowKey2, 18);
impl_row_key!(RowKey4, 36);
impl_row_key!(RowKey8, 72);
// nothing would prevent creation of RowKey16, RowKey32, etc., but they are rarely if ever needed
