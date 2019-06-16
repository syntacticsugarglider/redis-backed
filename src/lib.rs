//! redis-backed provides ergonomic, idiomatic, and featureful asynchronous
//! datastructures backed by the redis in-memory database engine
#![warn(
    missing_copy_implementations,
    anonymous_parameters,
    bare_trait_objects,
    elided_lifetimes_in_paths,
    missing_docs,
    trivial_numeric_casts,
    unreachable_pub,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]

use failure::Fail;

/// A database communication error.
#[derive(Fail, Debug)]
pub enum Error {
    /// An error reported by the database engine.
    #[fail(display = "A redis error occurred")]
    RedisError(#[cause] redis::RedisError),
    /// An error reported by the serialization process.
    #[fail(display = "A serialization error occurred")]
    SerializationError(#[cause] serde_cbor::error::Error),
}

impl From<redis::RedisError> for Error {
    fn from(error: redis::RedisError) -> Error {
        Error::RedisError(error)
    }
}

impl From<serde_cbor::error::Error> for Error {
    fn from(error: serde_cbor::error::Error) -> Error {
        Error::SerializationError(error)
    }
}

mod database;
pub use database::Database;

/// Provides types wrapping a variety of redis data structures.
pub mod collections;
