/// A redis-backed list collection.
pub mod list;

/// A redis-backed set collection.
pub mod set;

use redis::{Connection, ConnectionLike, RedisError};

use futures::{lazy, task::AtomicTask, Async, Future, Poll, Stream};

use std::{
    fmt::Debug,
    str::FromStr,
    sync::{Arc, RwLock},
};

use crossbeam_channel::{unbounded, Receiver, TryRecvError};

use crate::Error;

pub use list::List;
pub use set::Set;

/// Generic notification events that apply to all types of keys.
#[derive(Debug, Clone, Copy)]
pub enum GenericWatchEvent {
    /// The key was removed from the database.
    Remove,
}

/// An event produced by a modification of a watched key.
#[derive(Debug, Clone)]
pub enum WatchEvent<T: Send + Debug> {
    /// A generic event that applies to all key types.
    Generic(GenericWatchEvent),
    /// An event that applies only to a specific key type.
    TypeSpecific(T),
}

impl<T: Send + Debug + FromStr<Err = Error>> FromStr for WatchEvent<T> {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "del" => Ok(WatchEvent::Generic(GenericWatchEvent::Remove)),
            _ => Ok(WatchEvent::TypeSpecific(s.parse::<T>()?)),
        }
    }
}

/// A watcher that provides a stream of update notifications for a redis key.
pub struct Watcher<T: Send + Debug> {
    receiver: Receiver<Result<Option<WatchEvent<T>>, Error>>,
    task: Arc<AtomicTask>,
}

impl<'a, T: Send + Debug + FromStr<Err = Error> + 'static> Watcher<T> {
    fn watch(conn: Arc<RwLock<Connection>>, key: String) -> Watcher<T> {
        let (sender, receiver) = unbounded();
        let task = Arc::new(AtomicTask::new());
        let task_cloned = task.clone();
        tokio::spawn(lazy(move || {
            let mut conn = conn.write().unwrap();
            let db = conn.get_db();
            let mut pubsub = conn.as_pubsub();
            pubsub
                .subscribe(format!("__keyspace@{}__:{}", db, key))
                .unwrap();
            loop {
                let message = pubsub.get_message().unwrap();
                let payload: String = message.get_payload().unwrap();
                let event = payload.parse::<WatchEvent<T>>().map(|event| Some(event));
                sender.send(event).unwrap();
                task_cloned.notify();
            }
            Ok(())
        }));
        let watcher = Watcher { receiver, task };
        watcher
    }
}

impl<T: Send + Debug> Stream for Watcher<T> {
    type Item = WatchEvent<T>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.receiver.try_recv() {
            Ok(event) => match event {
                Ok(event) => Ok(Async::Ready(event)),
                Err(err) => Err(err),
            },
            Err(err) => match err {
                TryRecvError::Disconnected => panic!("watcher channel disconnected"),
                TryRecvError::Empty => {
                    self.task.register();
                    Ok(Async::NotReady)
                }
            },
        }
    }
}

/// A redis-backed data structure.
pub trait Collection<'a>: Key<<Self as Collection<'a>>::WatchEvent> {
    #[doc(hidden)]
    fn get(key: String, connection: Connection) -> Result<Self, RedisError>
    where
        Self: Sized;
    #[doc(hidden)]
    fn key(&self) -> String;
    #[doc(hidden)]
    fn connection(&self) -> Arc<RwLock<Connection>>;
    /// The structure-specific event type associated with this collection.
    type WatchEvent: Send + 'static + Debug + FromStr<Err = Error>;
}

/// A redis key with a variety of generic operations.
pub trait Key<T: Send + Debug> {
    /// Removes this key from the database.
    fn remove(self) -> Box<dyn Future<Item = (), Error = Error> + Send>;
    /// Begins watching this key for changes and updates.
    fn watch(&self) -> Box<dyn Future<Item = Watcher<T>, Error = Error> + Send>;
}

impl<'a, T> Key<T::WatchEvent> for T
where
    T: Collection<'a>,
{
    /// Removes the collection from the database. This operation is O(1).
    fn remove(self) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        let key = self.key();
        let connection = self.connection();
        Box::new(lazy(move || {
            let _: String = redis::cmd("DEL")
                .arg(key)
                .query(&mut *connection.write().unwrap())?;
            Ok(())
        }))
    }
    fn watch(&self) -> Box<dyn Future<Item = Watcher<T::WatchEvent>, Error = Error> + Send> {
        let connection = self.connection();
        let key = self.key();
        Box::new(lazy(move || Ok(Watcher::watch(connection, key))))
    }
}
