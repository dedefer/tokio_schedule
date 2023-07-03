/*!
This crate provides a library for scheduling periodic tasks.
It is inspired by python's [schedule](https://pypi.org/project/schedule/) lib and provides similar API.
It is built on tokio (version 1) and chrono lib

## Example

```rust
use std::error::Error;
use chrono::{Utc, Duration, Weekday};
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

    let every_second_1_day = every(1).second().until(&(Utc::now() + Duration::days(1)))
        .in_timezone(&Utc).perform(|| async { println!("Every second until next day") });
    spawn(every_second_1_day);

    let every_day = every(1).day().at(10, 00, 00)
        .in_timezone(&Utc).perform(|| async { println!("I'm scheduled!") });
    every_day.await;

    Ok(())
}

```

## Timezone

By default all jobs use Local timezone.
You can use any timezone that implements chrono::Timezone.
*/
use std::{
    future::Future,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Datelike, Local, TimeZone, Timelike, Weekday};
use tokio::time::sleep;

fn tz_now<TZ: TimeZone>(tz: &TZ) -> DateTime<TZ> {
    let sys_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    tz.timestamp_nanos(sys_ts.as_nanos() as i64)
}

fn cyclic_time_to_sleep_at<TZ: TimeZone>(
    now: &DateTime<TZ>,
    (hour, minute, second): (Option<u32>, Option<u32>, u32),
    (interval_step, interval_current, interval_mul): (u32, u32, u32),
) -> Duration {
    // this part represents position in current minute/hour/day relative to target time
    let offset_nanos = {
        let mut nanos = second as i64 * 1_000_000_000
            - (now.second() as i64 * 1_000_000_000 + now.nanosecond() as i64);

        if let Some(minute) = minute {
            let minutes = minute as i64 - now.minute() as i64;
            nanos += minutes * 60 * 1_000_000_000;
        }

        if let Some(hour) = hour {
            let hour = hour as i64 - now.hour() as i64;
            nanos += hour * 60 * 60 * 1_000_000_000;
        }

        nanos
    };
    // this part represents position in interval cycle
    // (e.g. in every(2).days it represents offset relative to nearest even day)
    let interval_nanos = match interval_current % interval_step {
        interval_offset
            // offset_nanos <= 0 means that we've passed target time in current minute/hour/day
            // that is we must complete cycle
            // interval_offset > 0 means that we've in the middle of cycle
            if (offset_nanos <= 0 || interval_offset > 0) =>
                interval_step - interval_offset,
        // this means that target time is ahead in current minute/hour/day
        // and we are at the beginning of the cycle (in the proper minute/hour/day)
        _ => 0,
    } as i64
        * interval_mul as i64
        * 1_000_000_000;

    Duration::from_nanos((interval_nanos + offset_nanos) as u64)
}

/// This Trait represents job in timezone.
/// It provides method perform.
pub trait Job: Sized + Sync {
    type TZ: TimeZone + Send + Sync;
    type UntilTZ: TimeZone + Send + Sync;

    /// This method acts like time_to_sleep but with custom reference time
    fn time_to_sleep_at(&self, now: &DateTime<Self::TZ>) -> Duration;

    #[doc(hidden)]
    fn timezone(&self) -> &Self::TZ;

    #[doc(hidden)]
    fn get_until(&self) -> Option<&DateTime<Self::UntilTZ>>;

    #[doc(hidden)]
    fn time_to_sleep_at_until(&self, now: &DateTime<Self::TZ>) -> Option<Duration> {
        let dur = self.time_to_sleep_at(now);
        let next_run = now.clone() + chrono::Duration::from_std(dur).unwrap();
        match self.get_until() {
            Some(until) if next_run.naive_utc() <= until.naive_utc() => Some(dur),
            Some(_) => None,
            None => Some(dur),
        }
    }

    /// This method returns Duration from now to next job run
    fn time_to_sleep(&self) -> Option<Duration> {
        self.time_to_sleep_at_until(&tz_now(self.timezone()))
    }

    /// This method returns Future that cyclic performs the job
    fn perform<'a, F, Fut>(self, mut func: F) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where
        Self: Send + 'a,
        F: FnMut() -> Fut + Send + 'a,
        Fut: Future<Output = ()> + Send + 'a,
        <Self::TZ as TimeZone>::Offset: Send + 'a,
    {
        let fut = async move {
            while let Some(dur) = self.time_to_sleep() {
                sleep(dur).await;
                func().await;
            }
        };

        Box::pin(fut)
    }
}

/// This function creates Every struct.
/// It is entrypoint for periodic jobs.
pub fn every(period: u32) -> Every {
    Every { step: period }
}

/// This is a builder for periodic jobs
#[derive(Debug, Clone)]
pub struct Every {
    step: u32,
}

impl Every {
    pub fn nanosecond(self) -> EveryNanosecond<Local, Local> {
        EveryNanosecond {
            step: self.step as u64,
            tz: Local,
            until: None,
        }
    }

    pub fn nanoseconds(self) -> EveryNanosecond<Local, Local> {
        self.nanosecond()
    }

    pub fn millisecond(self) -> EveryMillisecond<Local, Local> {
        EveryMillisecond {
            step: self.step as u64,
            tz: Local,
            until: None,
        }
    }

    pub fn milliseconds(self) -> EveryMillisecond<Local, Local> {
        self.millisecond()
    }

    pub fn second(self) -> EverySecond<Local, Local> {
        EverySecond {
            step: self.step,
            tz: Local,
            until: None,
        }
    }
    pub fn seconds(self) -> EverySecond<Local, Local> {
        self.second()
    }

    pub fn minute(self) -> EveryMinute<Local, Local> {
        EveryMinute {
            step: self.step,
            tz: Local,
            until: None,
            second: 00,
        }
    }
    pub fn minutes(self) -> EveryMinute<Local, Local> {
        self.minute()
    }

    pub fn hour(self) -> EveryHour<Local, Local> {
        EveryHour {
            step: self.step,
            tz: Local,
            until: None,
            minute: 00,
            second: 00,
        }
    }
    pub fn hours(self) -> EveryHour<Local, Local> {
        self.hour()
    }

    pub fn day(self) -> EveryDay<Local, Local> {
        EveryDay {
            step: self.step,
            tz: Local,
            until: None,
            hour: 00,
            minute: 00,
            second: 00,
        }
    }
    pub fn days(self) -> EveryDay<Local, Local> {
        self.day()
    }

    pub fn week(self) -> EveryWeekDay<Local, Local> {
        EveryWeekDay {
            step: self.step,
            tz: Local,
            until: None,
            weekday: Weekday::Mon,
            hour: 00,
            minute: 00,
            second: 00,
        }
    }
    pub fn weeks(self) -> EveryWeekDay<Local, Local> {
        self.week()
    }
}

#[derive(Debug, Clone)]
pub struct EveryNanosecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    step: u64, // must be > 0
    tz: TZ,
    until: Option<DateTime<UntilTZ>>,
}

impl<TZ, UntilTZ> EveryNanosecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    pub fn until<NewUntilTZ>(self, dt: &DateTime<NewUntilTZ>) -> EveryNanosecond<TZ, NewUntilTZ>
    where
        NewUntilTZ: Clone + TimeZone + Send + Sync,
    {
        EveryNanosecond {
            step: self.step,
            tz: self.tz,
            until: Some(dt.clone()),
        }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryNanosecond<NewTZ, UntilTZ>
    where
        NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryNanosecond {
            step: self.step,
            tz: new_tz.clone(),
            until: self.until,
        }
    }
}

impl<TZ, UntilTZ> Job for EveryNanosecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
    UntilTZ::Offset: Sync,
{
    type TZ = TZ;
    type UntilTZ = UntilTZ;

    fn timezone(&self) -> &Self::TZ {
        &self.tz
    }

    fn get_until(&self) -> Option<&DateTime<Self::UntilTZ>> {
        (&self.until).into()
    }

    fn time_to_sleep_at(&self, now: &DateTime<Self::TZ>) -> Duration {
        // calculate in nanoseconds
        let now_nanos = now.second() as u64 * 1_000_000_000 + now.nanosecond() as u64;

        let nanoseconds = match self.step {
            step_nanos if step_nanos > 0u64 => step_nanos - now_nanos % step_nanos,
            _ => 0,
        } as u64;

        Duration::from_nanos(nanoseconds)
    }
}

#[derive(Debug, Clone)]
pub struct EveryMillisecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    step: u64, // must be > 0
    tz: TZ,
    until: Option<DateTime<UntilTZ>>,
}

impl<TZ, UntilTZ> EveryMillisecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    pub fn until<NewUntilTZ>(self, dt: &DateTime<NewUntilTZ>) -> EveryMillisecond<TZ, NewUntilTZ>
    where
        NewUntilTZ: Clone + TimeZone + Send + Sync,
    {
        EveryMillisecond {
            step: self.step,
            tz: self.tz,
            until: Some(dt.clone()),
        }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryMillisecond<NewTZ, UntilTZ>
    where
        NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryMillisecond {
            step: self.step,
            tz: new_tz.clone(),
            until: self.until,
        }
    }
}

impl<TZ, UntilTZ> Job for EveryMillisecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
    UntilTZ::Offset: Sync,
{
    type TZ = TZ;
    type UntilTZ = UntilTZ;

    fn timezone(&self) -> &Self::TZ {
        &self.tz
    }

    fn get_until(&self) -> Option<&DateTime<Self::UntilTZ>> {
        (&self.until).into()
    }

    fn time_to_sleep_at(&self, now: &DateTime<Self::TZ>) -> Duration {
        // calculate in milliseconds
        let now_millis = now.second() as u64 * 1_000 + (now.nanosecond() as u64 / 1_000_000);
        let milliseconds = match self.step {
            step_millis if step_millis > 0u64 => step_millis - now_millis % step_millis,
            _ => 0,
        } as u64;

        Duration::from_millis(milliseconds)
    }
}

#[derive(Debug, Clone)]
pub struct EverySecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    step: u32, // must be > 0
    tz: TZ,
    until: Option<DateTime<UntilTZ>>,
}

impl<TZ, UntilTZ> EverySecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    pub fn until<NewUntilTZ>(self, dt: &DateTime<NewUntilTZ>) -> EverySecond<TZ, NewUntilTZ>
    where
        NewUntilTZ: Clone + TimeZone + Send + Sync,
    {
        EverySecond {
            step: self.step,
            tz: self.tz,
            until: Some(dt.clone()),
        }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EverySecond<NewTZ, UntilTZ>
    where
        NewTZ: Clone + TimeZone + Send + Sync,
    {
        EverySecond {
            step: self.step,
            tz: new_tz.clone(),
            until: self.until,
        }
    }
}

impl<TZ, UntilTZ> Job for EverySecond<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
    UntilTZ::Offset: Sync,
{
    type TZ = TZ;
    type UntilTZ = UntilTZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        // hack for leap second
        let nanos_part = (1_000_000_000 - now.nanosecond() % 1_000_000_000) as u64;

        let seconds_part = match self.step {
            step if step > 0 => step - 1 - now.second() % step,
            _ => 0,
        } as u64;

        Duration::from_nanos(seconds_part * 1_000_000_000 + nanos_part)
    }

    fn timezone(&self) -> &Self::TZ {
        &self.tz
    }

    fn get_until(&self) -> Option<&DateTime<Self::UntilTZ>> {
        (&self.until).into()
    }
}

#[derive(Debug, Clone)]
pub struct EveryMinute<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    step: u32,
    tz: TZ,
    until: Option<DateTime<UntilTZ>>,

    second: u32,
}

impl<TZ, UntilTZ> EveryMinute<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, second: u32) -> Self {
        Self { second, ..self }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryMinute<NewTZ, UntilTZ>
    where
        NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryMinute {
            step: self.step,
            tz: new_tz.clone(),
            until: self.until,
            second: self.second,
        }
    }

    pub fn until<NewUntilTZ>(self, dt: &DateTime<NewUntilTZ>) -> EveryMinute<TZ, NewUntilTZ>
    where
        NewUntilTZ: Clone + TimeZone + Send + Sync,
    {
        EveryMinute {
            step: self.step,
            tz: self.tz,
            until: Some(dt.clone()),
            second: self.second,
        }
    }
}

impl<TZ, UntilTZ> Job for EveryMinute<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
    UntilTZ::Offset: Sync,
{
    type TZ = TZ;
    type UntilTZ = UntilTZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        cyclic_time_to_sleep_at(
            now,
            (None, None, self.second),
            (self.step, now.minute(), 60),
        )
    }

    fn timezone(&self) -> &Self::TZ {
        &self.tz
    }

    fn get_until(&self) -> Option<&DateTime<Self::UntilTZ>> {
        (&self.until).into()
    }
}

pub struct EveryHour<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    step: u32,
    tz: TZ,
    until: Option<DateTime<UntilTZ>>,

    minute: u32,
    second: u32,
}

impl<TZ, UntilTZ> EveryHour<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, minute: u32, second: u32) -> Self {
        Self {
            minute,
            second,
            ..self
        }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryHour<NewTZ, UntilTZ>
    where
        NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryHour {
            step: self.step,
            tz: new_tz.clone(),
            until: self.until,
            minute: self.minute,
            second: self.second,
        }
    }

    pub fn until<NewUntilTZ>(self, dt: &DateTime<NewUntilTZ>) -> EveryHour<TZ, NewUntilTZ>
    where
        NewUntilTZ: Clone + TimeZone + Send + Sync,
    {
        EveryHour {
            step: self.step,
            tz: self.tz,
            until: Some(dt.clone()),
            minute: self.minute,
            second: self.second,
        }
    }
}

impl<TZ, UntilTZ> Job for EveryHour<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
    UntilTZ::Offset: Sync,
{
    type TZ = TZ;
    type UntilTZ = UntilTZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        cyclic_time_to_sleep_at(
            now,
            (None, Some(self.minute), self.second),
            (self.step, now.hour(), 60 * 60),
        )
    }

    fn timezone(&self) -> &Self::TZ {
        &self.tz
    }

    fn get_until(&self) -> Option<&DateTime<UntilTZ>> {
        (&self.until).into()
    }
}

#[derive(Debug, Clone)]
pub struct EveryDay<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    step: u32,
    tz: TZ,
    until: Option<DateTime<UntilTZ>>,

    hour: u32,
    minute: u32,
    second: u32,
}

impl<TZ, UntilTZ> EveryDay<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, hour: u32, minute: u32, second: u32) -> Self {
        Self {
            hour,
            minute,
            second,
            ..self
        }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryDay<NewTZ, UntilTZ>
    where
        NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryDay {
            step: self.step,
            tz: new_tz.clone(),
            until: self.until,
            hour: self.hour,
            minute: self.minute,
            second: self.second,
        }
    }

    pub fn until<NewUntilTZ>(self, dt: &DateTime<NewUntilTZ>) -> EveryDay<TZ, NewUntilTZ>
    where
        NewUntilTZ: Clone + TimeZone + Send + Sync,
    {
        EveryDay {
            step: self.step,
            tz: self.tz,
            until: Some(dt.clone()),
            hour: self.hour,
            minute: self.minute,
            second: self.second,
        }
    }
}

impl<TZ, UntilTZ> Job for EveryDay<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
    UntilTZ::Offset: Sync,
{
    type TZ = TZ;
    type UntilTZ = UntilTZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        cyclic_time_to_sleep_at(
            now,
            (Some(self.hour), Some(self.minute), self.second),
            (self.step, now.day(), 24 * 60 * 60),
        )
    }

    fn timezone(&self) -> &Self::TZ {
        &self.tz
    }

    fn get_until(&self) -> Option<&DateTime<Self::UntilTZ>> {
        (&self.until).into()
    }
}

#[derive(Debug, Clone)]
pub struct EveryWeekDay<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    step: u32,
    weekday: Weekday,
    tz: TZ,
    until: Option<DateTime<UntilTZ>>,

    hour: u32,
    minute: u32,
    second: u32,
}

impl<TZ, UntilTZ> EveryWeekDay<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, hour: u32, minute: u32, second: u32) -> Self {
        Self {
            hour,
            minute,
            second,
            ..self
        }
    }

    pub fn on(self, weekday: Weekday) -> Self {
        Self { weekday, ..self }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryWeekDay<NewTZ, UntilTZ>
    where
        NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryWeekDay {
            step: self.step,
            tz: new_tz.clone(),
            until: self.until,
            weekday: self.weekday,
            hour: self.hour,
            minute: self.minute,
            second: self.second,
        }
    }

    pub fn until<NewUntilTZ>(self, dt: &DateTime<NewUntilTZ>) -> EveryWeekDay<TZ, NewUntilTZ>
    where
        NewUntilTZ: Clone + TimeZone + Send + Sync,
    {
        EveryWeekDay {
            step: self.step,
            tz: self.tz,
            until: Some(dt.clone()),
            weekday: self.weekday,
            hour: self.hour,
            minute: self.minute,
            second: self.second,
        }
    }
}

impl<TZ, UntilTZ> Job for EveryWeekDay<TZ, UntilTZ>
where
    TZ: Clone + TimeZone + Send + Sync,
    UntilTZ: Clone + TimeZone + Send + Sync,
    UntilTZ::Offset: Sync,
{
    type TZ = TZ;
    type UntilTZ = UntilTZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        let offset_nanos = {
            let nanos = self.second as i64 * 1_000_000_000
                - (now.second() as i64 * 1_000_000_000 + now.nanosecond() as i64);
            let minutes = self.minute as i64 - now.minute() as i64;
            let hours = self.hour as i64 - now.hour() as i64;
            (hours * 60 + minutes) * 60 * 1_000_000_000 + nanos
        };

        let interval_nanos = {
            let current_week = now.iso_week().week();
            let current_week_in_cycle = current_week % self.step;
            let skip_in_week = self.weekday.number_from_monday() as i32
                - now.weekday().number_from_monday() as i32;

            if (skip_in_week > 0 && current_week_in_cycle > 0)
                || (skip_in_week == 0
                    && (current_week_in_cycle == 0 && offset_nanos <= 0
                        || current_week_in_cycle > 0))
                || (skip_in_week < 0)
            {
                skip_in_week + 7 * (self.step - current_week_in_cycle) as i32
            } else {
                skip_in_week
            }
        } as i64
            * 24
            * 60
            * 60
            * 1_000_000_000;

        Duration::from_nanos((interval_nanos + offset_nanos) as u64)
    }

    fn timezone(&self) -> &Self::TZ {
        &self.tz
    }

    fn get_until(&self) -> Option<&DateTime<Self::UntilTZ>> {
        (&self.until).into()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{DateTime, NaiveDate, Utc, Weekday};
    use regex::Regex;

    use crate::{every, Job};

    #[test]
    fn every_nanosecond() {
        assert_eq!(
            every(20)
                .nanoseconds()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt_ns(10, 30, 50, 30))
                .unwrap()
                .as_nanos(),
            10
        );

        assert_eq!(
            every(20)
                .nanoseconds()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt_ms(10, 30, 50, 60))
                .unwrap()
                .as_nanos(),
            20
        );
    }

    #[test]
    fn every_millisecond() {
        assert_eq!(
            every(30_000)
                .millisecond()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 30, 10))
                .unwrap()
                .as_millis(),
            20_000
        );

        assert_eq!(
            every(20)
                .milliseconds()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt_ms(10, 30, 50, 30))
                .unwrap()
                .as_millis(),
            10
        );

        assert_eq!(
            every(20)
                .milliseconds()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt_ms(10, 30, 50, 60))
                .unwrap()
                .as_millis(),
            20
        );

        assert_eq!(
            every(35)
                .milliseconds()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt_ms(10, 30, 50, 259))
                .unwrap()
                .as_millis(),
            1
        );
    }

    #[test]
    fn every_second() {
        assert_eq!(
            every(30)
                .seconds()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 30, 10))
                .unwrap()
                .as_secs(),
            "20s".secs()
        );

        assert_eq!(
            every(30)
                .seconds()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 30, 50))
                .unwrap()
                .as_secs(),
            "10s".secs()
        );
    }

    #[test]
    fn every_minute() {
        assert_eq!(
            every(30)
                .minutes()
                .at(20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 30, 10))
                .unwrap()
                .as_secs(),
            "10s".secs()
        );

        assert_eq!(
            every(30)
                .minutes()
                .at(20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 20, 10))
                .unwrap()
                .as_secs(),
            "10m 10s".secs()
        );

        assert_eq!(
            every(30)
                .minutes()
                .at(20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 20, 50))
                .unwrap()
                .as_secs(),
            "9m 30s".secs()
        );

        assert_eq!(
            every(1)
                .minutes()
                .at(20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 20, 10))
                .unwrap()
                .as_secs(),
            "10s".secs()
        );

        assert_eq!(
            every(1)
                .minutes()
                .at(20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 20, 30))
                .unwrap()
                .as_secs(),
            "50s".secs()
        );

        assert_eq!(
            every(1)
                .minutes()
                .at(20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 21, 20))
                .unwrap()
                .as_secs(),
            "1m".secs()
        );
    }

    #[test]
    fn every_hour() {
        assert_eq!(
            every(3)
                .hours()
                .at(10, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 30, 10))
                .unwrap()
                .as_secs(),
            "1h 40m 10s".secs()
        );

        assert_eq!(
            every(3)
                .hours()
                .at(10, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(23, 20, 50))
                .unwrap()
                .as_secs(),
            "49m 30s".secs()
        );

        assert_eq!(
            every(3)
                .hours()
                .at(10, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 20, 50))
                .unwrap()
                .as_secs(),
            "1h 49m 30s".secs()
        );

        assert_eq!(
            every(3)
                .hours()
                .at(10, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(12, 09, 10))
                .unwrap()
                .as_secs(),
            "1m 10s".secs()
        );

        assert_eq!(
            every(1)
                .hours()
                .at(10, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(23, 05, 00))
                .unwrap()
                .as_secs(),
            "5m 20s".secs()
        );

        assert_eq!(
            every(1)
                .hours()
                .at(10, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(00, 10, 20))
                .unwrap()
                .as_secs(),
            "1h".secs()
        );
    }

    #[test]
    fn every_day() {
        assert_eq!(
            every(2)
                .days()
                .at(10, 20, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 30, 10))
                .unwrap()
                .as_secs(),
            "23h 50m 10s".secs()
        );

        assert_eq!(
            every(2)
                .days()
                .at(10, 20, 20)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(23, 20, 50))
                .unwrap()
                .as_secs(),
            "10h 59m 30s".secs()
        );

        assert_eq!(
            every(1)
                .day()
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(23, 20, 50))
                .unwrap()
                .as_secs(),
            "39m 10s".secs()
        );

        assert_eq!(
            every(1)
                .day()
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(00, 00, 00))
                .unwrap()
                .as_secs(),
            "10h".secs()
        );

        assert_eq!(
            every(1)
                .day()
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(11, 00, 00))
                .unwrap()
                .as_secs(),
            "23h".secs()
        );

        assert_eq!(
            every(1)
                .day()
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 00, 00))
                .unwrap()
                .as_secs(),
            "24h".secs()
        );

        assert_eq!(
            every(1)
                .day()
                .at(10, 10, 50)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 09, 50))
                .unwrap()
                .as_secs(),
            "1m".secs()
        );

        assert_eq!(
            every(5)
                .day()
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(11, 00, 00))
                .unwrap()
                .as_secs(),
            "1d 23h".secs()
        );
    }

    #[test]
    fn every_weekday() {
        assert_eq!(
            every(1)
                .week()
                .on(Weekday::Wed)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(00, 00, 01))
                .unwrap()
                .as_secs(),
            "6d 23h 59m 59s".secs()
        );

        assert_eq!(
            every(1)
                .week()
                .on(Weekday::Thu)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 00, 00))
                .unwrap()
                .as_secs(),
            "1d".secs()
        );

        assert_eq!(
            every(1)
                .week()
                .on(Weekday::Wed)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 00, 00))
                .unwrap()
                .as_secs(),
            "7d".secs()
        );

        assert_eq!(
            every(1)
                .week()
                .on(Weekday::Mon)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&dt(10, 00, 00))
                .unwrap()
                .as_secs(),
            "5d".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Wed)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(09, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 1d 1h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Wed)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(11, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 23h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Wed)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(10, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 1d".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Mon)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(09, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "2w 6d 1h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Mon)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(11, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "2w 5d 23h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Mon)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(10, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "2w 6d".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Tue)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(09, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 1h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Tue)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(11, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "2w 6d 23h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Tue)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 2'nd week in cycle, Tue
                    NaiveDate::from_ymd(2021, 02, 02).and_hms(10, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Wed)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(09, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "1d 1h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Wed)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(11, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "23h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Wed)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(10, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "1d".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Mon)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(09, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 6d 1h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Mon)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(11, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 5d 23h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Mon)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(10, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 6d".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Tue)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(09, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "1h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Tue)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(11, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "3w 6d 23h".secs()
        );

        assert_eq!(
            every(4)
                .weeks()
                .on(Weekday::Tue)
                .at(10, 00, 00)
                .in_timezone(&Utc)
                .time_to_sleep_at_until(&DateTime::from_utc(
                    // 1st week in cycle, Tue
                    NaiveDate::from_ymd(2021, 01, 26).and_hms(10, 00, 00),
                    Utc
                ))
                .unwrap()
                .as_secs(),
            "4w".secs()
        );
    }

    #[test]
    fn until() {
        assert!(every(30)
            .seconds()
            .until(&dt(10, 30, 09))
            .in_timezone(&Utc)
            .time_to_sleep_at_until(&dt(10, 30, 10))
            .is_none());

        assert!(every(30)
            .minutes()
            .at(20)
            .until(&dt(10, 30, 09))
            .in_timezone(&Utc)
            .time_to_sleep_at_until(&dt(10, 30, 10))
            .is_none());

        assert!(every(3)
            .hours()
            .at(10, 20)
            .until(&dt(10, 30, 09))
            .in_timezone(&Utc)
            .time_to_sleep_at_until(&dt(10, 30, 10))
            .is_none());

        assert!(every(2)
            .days()
            .at(10, 20, 20)
            .until(&dt(10, 30, 09))
            .in_timezone(&Utc)
            .time_to_sleep_at_until(&dt(10, 30, 10))
            .is_none());

        assert!(every(1)
            .week()
            .on(Weekday::Wed)
            .until(&dt(00, 00, 00))
            .in_timezone(&Utc)
            .time_to_sleep_at_until(&dt(00, 00, 01))
            .is_none());
    }

    trait ParseToSecs {
        fn secs(&self) -> u64;
    }

    impl ParseToSecs for str {
        fn secs(&self) -> u64 {
            let re =
                Regex::new(r#"(?:(\d)w)?\s*(?:(\d)d)?\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?"#)
                    .unwrap();
            let caps = re.captures(self).unwrap();

            let mut res = 0u64;
            caps.get(1).map(|weeks| {
                res += u64::from_str(weeks.as_str()).unwrap() * 7 * 24 * 60 * 60;
            });
            caps.get(2).map(|days| {
                res += u64::from_str(days.as_str()).unwrap() * 24 * 60 * 60;
            });
            caps.get(3).map(|hours| {
                res += u64::from_str(hours.as_str()).unwrap() * 60 * 60;
            });
            caps.get(4).map(|minutes| {
                res += u64::from_str(minutes.as_str()).unwrap() * 60;
            });
            caps.get(5).map(|seconds| {
                res += u64::from_str(seconds.as_str()).unwrap();
            });

            res
        }
    }

    fn dt(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        let day = NaiveDate::from_ymd_opt(1998, 12, 23).unwrap();
        DateTime::from_utc(day.and_hms_opt(hour, minute, second).unwrap(), Utc)
    }

    fn dt_ms(hour: u32, minute: u32, second: u32, millisecond: u32) -> DateTime<Utc> {
        let day = NaiveDate::from_ymd_opt(1998, 12, 23).unwrap();
        DateTime::from_utc(
            day.and_hms_milli_opt(hour, minute, second, millisecond)
                .unwrap(),
            Utc,
        )
    }

    fn dt_ns(hour: u32, minute: u32, second: u32, nanosecond: u32) -> DateTime<Utc> {
        let day = NaiveDate::from_ymd_opt(1998, 12, 23).unwrap();
        DateTime::from_utc(
            day.and_hms_nano_opt(hour, minute, second, nanosecond)
                .unwrap(),
            Utc,
        )
    }
}
