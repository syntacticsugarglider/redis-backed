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
    /// sometimes referred to as the last element of the list. This operation is O(1).
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
    /// sometimes referred to as the first element of the list. This operation is O(1).
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
    /// Gets the element from the list at the provided index. The index is zero based
    /// (0 is the first element and so on) and negative numbers can be used to designate
    /// elements starting at the end/tail/right of the list (i.e. -1 is the last element and so forth).
    /// This function runs in O(n) over the distance of the provided index from the nearest
    /// end of the list i.e. getting the start or end of the list is O(1). If the specified element
    /// does not exist or the index is out of range an error will be returned.
    pub fn index(&mut self, index: i64) -> impl Future<Item = T, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: Vec<u8> = redis::cmd("LINDEX")
                .arg(key)
                .arg(index)
                .query(&mut *connection.write().unwrap())?;
            Ok(serde_cbor::from_slice(data.as_slice())?)
        })
    }
    /// Returns elements of the list starting at `start` and stopping at `stop` which are zero-based indices
    /// permitting negative values in the same way as `index`. Note that the rightmost item in any range is included (i.e. range(0, 10) returns 11 elements).
    /// This operation is O(S+N) where S is the distance of the start offset from the head (for small lists) or nearest end (for large lists) and N is the number of
    /// elements in the range specified.
    pub fn range(&mut self, start: i64, stop: i64) -> impl Future<Item = Vec<T>, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: Vec<Vec<u8>> = redis::cmd("LRANGE")
                .arg(key)
                .arg(start)
                .arg(stop)
                .query(&mut *connection.write().unwrap())?;
            data.iter()
                .map(|data| serde_cbor::from_slice(data.as_slice()).map_err(|err| Error::from(err)))
                .collect::<Result<Vec<T>, Error>>()
        })
    }
    /// Pushes an element to the front/right/tail/end of the list. This makes the provided
    /// element the last item of the list. This operation is O(1).
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
    /// element the first item of the list. This operation is O(1).
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
    /// Returns the length of the list. This operation executes in O(1) time.
    pub fn len(&mut self) -> impl Future<Item = u32, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: u32 = redis::cmd("LLEN")
                .arg(key)
                .query(&mut *connection.write().unwrap())?;
            Ok(data)
        })
    }
    /// O(N) over the length of the list. Removes the first `count` occurrences of elements equal to `item` from the list. For positive `count` elements are removed moving from head to tail,
    /// for negative `count` they are removed from tail to head, and for `count` equal to zero all elements are removed. This call returns the number of elements actually removed.
    pub fn remove(&mut self, count: u32, item: T) -> impl Future<Item = u32, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let data: u32 = redis::cmd("LREM")
                .arg(key)
                .arg(count)
                .arg(serde_cbor::to_vec(&item)?)
                .query(&mut *connection.write().unwrap())?;
            Ok(data)
        })
    }
}