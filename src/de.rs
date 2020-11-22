pub use avro_rs::util::{safe_len, zag_i32, zag_i64, DecodeError};
use failure::Error;
use rutie::*;
use std::io::Read;

#[inline]
pub fn decode_long<R: Read>(reader: &mut R) -> Result<AnyObject, Error> {
    zag_i64(reader).map(|i| Integer::new(i).into())
}

#[inline]
pub fn decode_int<R: Read>(reader: &mut R) -> Result<AnyObject, Error> {
    zag_i32(reader).map(|i| Integer::new(i as i64).into())
}

#[inline]
pub fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|len| safe_len(len as usize))
}
