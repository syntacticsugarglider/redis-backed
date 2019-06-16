mod list;

use redis::{Connection, RedisError};

pub use list::List;

/// A redis-backed data structure.
pub trait Collection<'a>: Sized {
    #[doc(hidden)]
    fn get(key: String, connection: Connection) -> Result<Self, RedisError>;
}
