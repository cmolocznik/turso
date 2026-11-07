//! What the completion port's requests take and its packets say (DESIGN.md 22.5). Free of the
//! platform, so its boundaries are tested on every one, though only the completion port uses it.

use crate::{CompletionError, LimboError, Result};
use std::io;

/// Windows' `ERROR_HANDLE_EOF` and `ERROR_OPERATION_ABORTED` (winerror.h); the completion port
/// asserts at compile time that they are windows-sys's.
pub(super) const ERROR_HANDLE_EOF: u32 = 38;
pub(super) const ERROR_OPERATION_ABORTED: u32 = 995;

/// A read's or write's length as the system call takes it, refused before the request is accepted
/// when its byte count, which the request's result carries as an `i32`, could not fit: a
/// submission refuses whole.
pub(super) fn request_len(len: usize) -> Result<u32> {
    i32::try_from(len).map(|len| len as u32).map_err(|_| {
        LimboError::InternalError(format!(
            "a request of {len} bytes, more than its result can carry"
        ))
    })
}

/// The result a packet delivers to its request: the bytes moved if it succeeded, else its error.
/// The packet is consumed by then, so whatever it says is delivered: a count past `i32::MAX`,
/// which no request accepted can move, and an error code past `i32::MAX` are delivered as errors,
/// never left with the request still owned.
pub(super) fn packet_result(
    succeeded: bool,
    error: u32,
    bytes: u32,
) -> Result<i32, CompletionError> {
    match (succeeded, error) {
        (true, _) => i32::try_from(bytes).map_err(|_| {
            CompletionError::IOError(io::ErrorKind::InvalidData, "byte count past i32::MAX")
        }),
        (false, ERROR_OPERATION_ABORTED) => Err(CompletionError::Aborted),
        (false, ERROR_HANDLE_EOF) => Err(CompletionError::IOError(
            io::ErrorKind::UnexpectedEof,
            "io-error-completion",
        )),
        (false, code) => Err(CompletionError::IOError(
            i32::try_from(code).map_or(io::ErrorKind::Other, |code| {
                io::Error::from_raw_os_error(code).kind()
            }),
            "io-error-completion",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A count one past what an `i32` holds is delivered as an error, not dropped with the
    /// request still owned (mutant: the conversion's failure returned before the delivery).
    #[test]
    fn a_packet_whose_count_is_past_i32_delivers_an_error() {
        assert_eq!(packet_result(true, 0, 512), Ok(512));
        assert_eq!(packet_result(true, 0, i32::MAX as u32), Ok(i32::MAX));
        assert_eq!(
            packet_result(true, 0, i32::MAX as u32 + 1),
            Err(CompletionError::IOError(
                io::ErrorKind::InvalidData,
                "byte count past i32::MAX"
            ))
        );
        assert_eq!(
            packet_result(false, ERROR_OPERATION_ABORTED, 0),
            Err(CompletionError::Aborted)
        );
        assert!(matches!(
            packet_result(false, u32::MAX, 0),
            Err(CompletionError::IOError(io::ErrorKind::Other, _))
        ));
    }

    /// A request longer than an `i32` counts is refused at its submission, before acceptance.
    #[test]
    fn a_request_longer_than_its_result_can_count_is_refused() {
        assert_eq!(request_len(4096).unwrap(), 4096);
        assert_eq!(request_len(i32::MAX as usize).unwrap(), i32::MAX as u32);
        assert!(request_len(i32::MAX as usize + 1).is_err());
    }
}
