![MIT licensed](https://img.shields.io/github/license/dedefer/tokio_schedule?style=for-the-badge)
[![Version](https://img.shields.io/crates/v/tokio_schedule?style=for-the-badge)](https://crates.io/crates/tokio_schedule/)
![Code Coverage](https://img.shields.io/coveralls/github/dedefer/tokio_schedule/main?style=for-the-badge)
![Downloads](https://img.shields.io/crates/d/tokio_schedule?style=for-the-badge)

This crate provides a library for scheduling periodic tasks.
It is inspired by python's [schedule](https://pypi.org/project/schedule/) lib and provides similar API.
It is built on tokio (version 1) and chrono lib

[Documentation link](https://docs.rs/tokio_schedule/)

[Crates.io link](https://crates.io/crates/tokio_schedule/)

## Example

```rust
use std::error::Error;
use chrono::{Utc, Weekday};
use tokio::spawn;
use tokio_schedule::{every, Job};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let weekly = every(1).week().on(Weekday::Mon).at(12, 00, 00)
        .in_timezone(&Utc).perform(|| async { println!("Every week job") });
    spawn(weekly);

    let even_weekly = every(2).weeks().on(Weekday::Mon).at(12, 00, 00)
        .in_timezone(&Utc).perform(|| async { println!("Every even week job") });
    spawn(even_weekly);

    let every_30_seconds = every(30).seconds() // by default chrono::Local timezone
        .perform(|| async { println!("Every minute at 00'th and 30'th second") });
    spawn(every_30_seconds);

    let every_30_minutes = every(30).minutes().at(20).in_timezone(&Utc)
        .perform(|| async { println!("Every 30 minutes at 20'th second") });
    spawn(every_30_minutes);

    let every_hour = every(1).hour().at(10, 30).in_timezone(&Utc)
        .perform(|| async { println!("Every hour at :10:30") });
    spawn(every_hour);

    let every_day = every(1).day().at(10, 00, 00)
        .in_timezone(&Utc).perform(|| async { println!("I'm scheduled!") });
    every_day.await;

    Ok(())
}

```

## Timezone

By default all jobs use Local timezone.
You can use any timezone that implements chrono::Timezone.
