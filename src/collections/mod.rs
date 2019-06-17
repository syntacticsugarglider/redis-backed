mod list;

use redis::{Connection, RedisError};

use futures::{lazy, Future};

use std::sync::{Arc, RwLock};

use crate::Error;

pub use list::List;

/// A redis-backed data structure.
pub trait Collection<'a> {
    #[doc(hidden)]
    fn get(key: String, connection: Connection) -> Result<Self, RedisError>
    where
        Self: Sized;
    #[doc(hidden)]
    fn key(&self) -> String;
    #[doc(hidden)]
    fn connection(&self) -> Arc<RwLock<Connection>>;
}

impl<'a> dyn Collection<'a> {
    /// Removes the collection from the database. This operation is O(1).
    pub fn remove(self) -> impl Future<Item = (), Error = Error>
    where
        Self: Sized,
    {
        let key = self.key();
        let connection = self.connection();
        lazy(move || {
            let _: String = redis::cmd("DEL")
                .arg(key)
                .query(&mut *connection.write().unwrap())?;
            Ok(())
        })
    }
}
