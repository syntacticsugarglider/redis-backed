use super::Collection;
use futures::{lazy, Future};
use redis::{Connection, RedisError};

use crate::Error;

use serde::{de::DeserializeOwned, Serialize};
use std::{
    marker::PhantomData,
    sync::{Arc, RwLock},
};

/// A redis-backed list wrapping the built-in redis List structure.
///
/// This data-structure behaves similarly to a VecDeque i.e. it is O(1)
/// to add/remove elements (push/pop) from the head and tail but O(n) over
/// the length of the list to insert/set at a specific index.
pub struct List<T: Serialize + DeserializeOwned> {
    connection: Arc<RwLock<Connection>>,
    key: String,
    data: PhantomData<T>,
}

impl<'a, T: Serialize + DeserializeOwned> Collection<'a> for List<T> {
    fn get(key: String, connection: Connection) -> Result<List<T>, RedisError> {
        Ok(List {
            key,
            connection: Arc::new(RwLock::new(connection)),
            data: PhantomData,
        })
    }
}

impl<T: Serialize + DeserializeOwned> List<T> {
    /// Pops an element from the front/right/tail/end of the list. This is also
    /// sometimes referred to as the last element of the list.
    pub fn pop_front(&mut self) -> impl Future<Item = Option<T>, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: Option<Vec<u8>> = redis::cmd("RPOP")
                .arg(key)
                .query(&mut *connection.write().unwrap())?;
            match data {
                None => Ok(None),
                Some(data) => Ok(serde_cbor::from_slice(data.as_slice())?),
            }
        })
    }
    /// Pops an element from the rear/left/head/start of the list. This is also
    /// sometimes referred to as the first element of the list.
    pub fn pop_back(&mut self) -> impl Future<Item = Option<T>, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: Option<Vec<u8>> = redis::cmd("LPOP")
                .arg(key)
                .query(&mut *connection.write().unwrap())?;
            match data {
                None => Ok(None),
                Some(data) => Ok(serde_cbor::from_slice(data.as_slice())?),
            }
        })
    }
    /// Pushes an element to the front/right/tail/end of the list. This makes the provided
    /// element the last item of the list.
    pub fn push_front(&mut self, item: T) -> impl Future<Item = (), Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: Vec<u8> = serde_cbor::to_vec(&item)?;
            redis::cmd("RPUSH")
                .arg(key)
                .arg(data)
                .query(&mut *connection.write().unwrap())?;
            Ok(())
        })
    }
    /// Pushes an element to the rear/left/head/start of the list. This makes the provided
    /// element the first item of the list.
    pub fn push_back(&mut self, item: T) -> impl Future<Item = (), Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: Vec<u8> = serde_cbor::to_vec(&item)?;
            redis::cmd("LPUSH")
                .arg(key)
                .arg(data)
                .query(&mut *connection.write().unwrap())?;
            Ok(())
        })
    }
}
