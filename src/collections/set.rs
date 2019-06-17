use super::Collection;
use futures::{lazy, Future};
use redis::{Connection, RedisError};

use crate::Error;

use serde::{de::DeserializeOwned, Serialize};
use std::{
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, RwLock},
};

/// Events that can occur on a Set.
#[derive(Debug, Clone, Copy)]
pub enum SetEvent {}

impl FromStr for SetEvent {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            _ => Err(Error::InvalidNotification {
                type_name: "Set".to_owned(),
                notification: s.to_owned(),
            }),
        }
    }
}

/// A redis-backed set wrapping the built-in redis Set data structure.
///
/// This is a hashset that stores one copy of each unique item and permits
/// low-cost O(1) existence checks and additions.
pub struct Set<T: Serialize + DeserializeOwned> {
    connection: Arc<RwLock<Connection>>,
    key: String,
    data: PhantomData<T>,
}

impl<'a, T: Serialize + DeserializeOwned> Collection<'a> for Set<T> {
    type WatchEvent = SetEvent;
    fn get(key: String, connection: Connection) -> Result<Set<T>, RedisError> {
        Ok(Set {
            key: format!("_orm_set:{}", key),
            connection: Arc::new(RwLock::new(connection)),
            data: PhantomData,
        })
    }
    fn key(&self) -> String {
        self.key.clone()
    }
    fn connection(&self) -> Arc<RwLock<Connection>> {
        self.connection.clone()
    }
}

impl<T: Serialize + DeserializeOwned> Set<T> {
    /// Adds the provided item to the set, returning `false` if it was already present
    /// and `true` otherwise. This operation is O(1).
    pub fn add(&mut self, item: T) -> impl Future<Item = bool, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let ret: u32 = redis::cmd("SADD")
                .arg(key)
                .arg(serde_cbor::to_vec(&item)?)
                .query(&mut *connection.write().unwrap())?;
            Ok(ret == 1)
        })
    }
    /// Returns the number of elements in the set or 0 if the set does not
    /// already exist. This operation is O(1).
    pub fn count(&mut self) -> impl Future<Item = u32, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let count: u32 = redis::cmd("SCARD")
                .arg(key)
                .query(&mut *connection.write().unwrap())?;
            Ok(count)
        })
    }
    /// Removes the provided item from the set, returning `false` if the item was not already present
    /// and `true` otherwise. This operation is O(1).
    pub fn remove(&mut self, item: T) -> impl Future<Item = bool, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let ret: u32 = redis::cmd("SREM")
                .arg(key)
                .arg(serde_cbor::to_vec(&item)?)
                .query(&mut *connection.write().unwrap())?;
            Ok(ret == 1)
        })
    }
    /// Checks if the provided key is a member of the set. Returns `true` if it is,
    /// false if it isn't. This operation is O(1).
    pub fn contains(&mut self, item: T) -> impl Future<Item = bool, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let ret: u32 = redis::cmd("SISMEMBER")
                .arg(key)
                .arg(serde_cbor::to_vec(&item)?)
                .query(&mut *connection.write().unwrap())?;
            Ok(ret == 1)
        })
    }
    /// Returns a vector containing all members of the set. This operation is O(N)
    /// over the number of elements in the set.
    pub fn to_vec(&mut self) -> impl Future<Item = Vec<T>, Error = Error> {
        let key = self.key.clone();
        let connection = self.connection.clone();
        lazy(move || {
            let ret: Vec<Vec<u8>> = redis::cmd("SMEMBERS")
                .arg(key)
                .query(&mut *connection.write().unwrap())?;
            ret.iter()
                .map(|data| serde_cbor::from_slice(data.as_slice()).map_err(|err| Error::from(err)))
                .collect::<Result<Vec<T>, Error>>()
        })
    }
}
