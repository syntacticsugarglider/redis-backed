use redis_backed::collections::Set;
use redis_backed::Database;
use serde::{Deserialize, Serialize};

use futures::Future;

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
                    .get::<Set<Person>>("people")
                    .map_err(|e| {
                        eprintln!("{:?}", e);
                        ()
                    })
                    .and_then(|mut set| {
                        set.add(Person {
                            name: "john".to_owned(),
                            age: 52,
                        })
                        .map_err(|e| {
                            eprintln!("{:?}", e);
                            ()
                        })
                        .and_then(|item| {
                            println!("{:?}", item);
                            Ok(())
                        })
                    })
            }),
    );
}