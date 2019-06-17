use redis_backed::collections::{Key, List};
use redis_backed::Database;
use serde::{Deserialize, Serialize};

use futures::{Future, Stream};

#[derive(Serialize, Deserialize, Debug)]
pub struct Person {
    name: String,
    age: u8,
}

fn main() {
    tokio::run(
        Database::new("redis://127.0.0.1/")
            .map_err(|e| {
                eprintln!("{:?}", e);
                ()
            })
            .and_then(|mut database| {
                database
                    .get::<List<Person>>("people")
                    .map_err(|e| {
                        eprintln!("{:?}", e);
                        ()
                    })
                    .and_then(|list| {
                        list.watch()
                            .map_err(|e| {
                                eprintln!("{:?}", e);
                                ()
                            })
                            .and_then(|watcher| {
                                watcher
                                    .map_err(|e| {
                                        eprintln!("{:?}", e);
                                        ()
                                    })
                                    .for_each(|event| {
                                        println!("{:?}", event);
                                        Ok(())
                                    })
                            })
                    })
            }),
    );
}
