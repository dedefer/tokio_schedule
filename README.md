This crate provides a library for scheduling periodic tasks.
It is inspired by python's [schedule](https://pypi.org/project/schedule/) lib and provides similar API.
It is built on tokio (version 1) and chrono lib

## Example

```rust
use std::error::Error;
use chrono::{Utc, Weekday};
use tokio::spawn;
use tokio_schedule::{every, every_week, Job};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let weekly = every_week(Weekday::Mon).at(12, 00, 00)
        .in_timezone(&Utc).perform(|| async { println!("Every week job") });
    spawn(weekly);

    let every_30_seconds = every(30).seconds() // by default chrono::Local timezone
        .perform(|| async { println!("Every minute at 00'th and 30'th second") })
    spawn(every_30_seconds);

    let every_30_minutes = every(30).minutes().at(20).in_timezone(&Utc)
        .perform(|| async { println!("Every 30 minutes at 20'th second") })

    let every_hour = every(1).hour().at(10, 30).in_timezone(&Utc)
        .perform(|| async { println!("Every hour at :10:30") })

    let every_day = every(1).day().at(10, 00, 00)
        .in_timezone(&Utc).perform(|| async { println!("I'm scheduled!") });
    every_day.await;

    Ok(())
}

```

## Timezone

By default all jobs use Local timezone.
You can use any timezone that implements chrono::Timezone.
