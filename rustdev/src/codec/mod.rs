//! Encoding and decoding utilities

pub mod comparator;
pub mod encoding;

pub use comparator::{Comparator, BytewiseComparator, ReverseBytewiseComparator};
pub use encoding::*;