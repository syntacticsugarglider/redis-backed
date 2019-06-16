use futures::{lazy, Future};

use redis::{Client, IntoConnectionInfo, RedisError};

use crate::collections::Collection;

use std::sync::{Arc, RwLock};

/// A redis database connection.
pub struct Database {
    client: Arc<RwLock<Client>>,
}

impl Database {
    /// Connects to a database at the provided address.
    ///
    /// This method will not fail even if no database is listening on
    /// the provided address or if the connection would otherwise fail. It checks to ensure the provided address is valid
    /// but no more, actual connection will not occur until an operation is performed.
    pub fn new<'a, T: IntoConnectionInfo + 'a>(
        addr: T,
    ) -> impl Future<Item = Database, Error = RedisError> + 'a {
        lazy(move || {
            let client = Arc::new(RwLock::new(Client::open(addr)?));
            Ok(Database { client })
        })
    }
    /// Gets a data structure of the provided type at the specified key.
    pub fn get<'a, T: Collection<'a> + 'a>(
        &'a mut self,
        name: &'a str,
    ) -> impl Future<Item = T, Error = RedisError> {
        let client = self.client.clone();
        let name = name.to_owned();
        lazy(move || {
            let conn = client.read().unwrap().get_connection()?;
            T::get(name, conn)
        })
    }
}
