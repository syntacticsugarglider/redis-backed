use redis_backed::collections::List;
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
                    .get::<List<Person>>("people")
                    .map_err(|e| {
                        eprintln!("{:?}", e);
                        ()
                    })
                    .and_then(|mut list| {
                        list.push_front(Person {
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