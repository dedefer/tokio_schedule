/*!
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
*/
use std::{future::Future, pin::Pin, time::{Duration, SystemTime, UNIX_EPOCH}};

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
        let mut nanos =
            second as i64 * 1_000_000_000 - (now.second() as i64 * 1_000_000_000 + now.nanosecond() as i64);

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
    } as i64 * interval_mul as i64 * 1_000_000_000;

    Duration::from_nanos((interval_nanos + offset_nanos) as u64)
}

/// This Trait represents job in timezone.
/// It provides method perform.
pub trait Job: Sized + Sync {
    type TZ: TimeZone + Send + Sync;

    /// This method acts like time_to_sleep but with custom reference time
    fn time_to_sleep_at(&self, now: &DateTime<Self::TZ>) -> Duration;

    #[doc(hidden)]
    fn timezone(&self) -> &Self::TZ;

    /// This method returns Duration from now to next job run
    fn time_to_sleep(&self) -> Duration {
        self.time_to_sleep_at(&tz_now(self.timezone()))
    }

    /// This method returns Future that cyclic performs the job
    fn perform<'a, F, Fut>(self, mut func: F) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where
        Self: Send + 'a,
        F: FnMut() -> Fut + Send + 'a,
        Fut: Future<Output = ()> + Send + 'a,
        <Self::TZ as TimeZone>::Offset: Send + 'a,
    {
        let fut = async move { loop {
            sleep(self.time_to_sleep()).await;
            func().await;
        }};

        Pin::from(Box::new(fut))
    }
}

/// This function creates Every struct.
/// It is entrypoint for periodic jobs.
pub fn every(period: u32) -> Every { Every { step: period } }

/// This function creates EveryWeekDay struct.
/// It is entrypoint for weekly jobs.
pub fn every_week(weekday: Weekday) -> EveryWeekDay<Local> {
    EveryWeekDay { tz: Local, weekday }
}

/// This is a builder for periodic jobs
#[derive(Debug, Clone)]
pub struct Every {
    step: u32,
}

impl Every {
    pub fn second(self) -> EverySecond<Local> {
        EverySecond { step: self.step, tz: Local }
    }
    pub fn seconds(self) -> EverySecond<Local> { self.second() }

    pub fn minute(self) -> EveryMinute<Local> {
        EveryMinute { step: self.step, tz: Local }
    }
    pub fn minutes(self) -> EveryMinute<Local> { self.minute() }

    pub fn hour(self) -> EveryHour<Local> {
        EveryHour { step: self.step, tz: Local }
    }
    pub fn hours(self) -> EveryHour<Local> { self.hour() }

    pub fn day(self) -> EveryDay<Local> {
        EveryDay { step: self.step, tz: Local }
    }
    pub fn days(self) -> EveryDay<Local> { self.day() }
}

#[derive(Debug, Clone)]
pub struct EverySecond<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    step: u32, // must be > 0
    tz: TZ,
}

impl<TZ> EverySecond<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EverySecond<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    { EverySecond { step: self.step, tz: new_tz.clone() } }
}

impl<TZ> Job for EverySecond<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        // hack for leap second
        let nanos_part =  (1_000_000_000 - now.nanosecond() % 1_000_000_000) as u64;

        let seconds_part = match self.step {
            step if step > 0 => step - 1 - now.second() % step,
            _ => 0,
        } as u64;

        Duration::from_nanos(seconds_part * 1_000_000_000 + nanos_part)
    }

    fn timezone(&self) -> &Self::TZ { &self.tz }
}

#[derive(Debug, Clone)]
pub struct EveryMinute<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    step: u32,
    tz: TZ,
}

impl<TZ> EveryMinute<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, second: u32) -> EveryMinuteAt<TZ> {
        EveryMinuteAt { minute: self, second }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryMinute<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    { EveryMinute { step: self.step, tz: new_tz.clone() } }
}

impl<TZ> Job for EveryMinute<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        EveryMinuteAt {
            minute: self.clone(),
            second: 00,
        }.time_to_sleep_at(now)
    }

    fn timezone(&self) -> &Self::TZ { &self.tz }
}

#[derive(Debug, Clone)]
pub struct EveryMinuteAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    minute: EveryMinute<TZ>,
    second: u32,
}

impl<TZ> EveryMinuteAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryMinuteAt<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryMinuteAt {
            minute: self.minute.in_timezone(new_tz),
            second: self.second,
        }
    }
}

impl<TZ> Job for EveryMinuteAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        cyclic_time_to_sleep_at(
            now, (None, None, self.second),
            (self.minute.step, now.minute(), 60),
        )
    }

    fn timezone(&self) -> &Self::TZ { &self.minute.tz }
}

#[derive(Debug, Clone)]
pub struct EveryHour<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    step: u32,
    tz: TZ,
}

impl<TZ> EveryHour<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, minute: u32, second: u32) -> EveryHourAt<TZ> {
        EveryHourAt { hour: self, minute, second }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryHour<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    { EveryHour { step: self.step, tz: new_tz.clone() } }
}

impl<TZ> Job for EveryHour<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        EveryHourAt {
            hour: self.clone(),
            minute: 00,
            second: 00,
        }.time_to_sleep_at(now)
    }

    fn timezone(&self) -> &Self::TZ { &self.tz }
}

pub struct EveryHourAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    hour: EveryHour<TZ>,
    minute: u32,
    second: u32,
}

impl<TZ> EveryHourAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryHourAt<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryHourAt {
            hour: self.hour.in_timezone(new_tz),
            minute: self.minute,
            second: self.second,
        }
    }
}

impl<TZ> Job for EveryHourAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        cyclic_time_to_sleep_at(
            now, (None, Some(self.minute), self.second),
            (self.hour.step, now.hour(), 60 * 60),
        )
    }

    fn timezone(&self) -> &Self::TZ { &self.hour.tz }
}

#[derive(Debug, Clone)]
pub struct EveryDay<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    step: u32,
    tz: TZ,
}

impl<TZ> EveryDay<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, hour: u32, minute: u32, second: u32) -> EveryDayAt<TZ> {
        EveryDayAt { day: self, hour, minute, second }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryDay<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    { EveryDay { step: self.step, tz: new_tz.clone() } }
}

impl<TZ> Job for EveryDay<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        EveryDayAt {
            day: self.clone(),
            hour: 00,
            minute: 00,
            second: 00,
        }.time_to_sleep_at(now)
    }

    fn timezone(&self) -> &Self::TZ { &self.tz }
}

#[derive(Debug, Clone)]
pub struct EveryDayAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    day: EveryDay<TZ>,
    hour: u32,
    minute: u32,
    second: u32,
}

impl<TZ> EveryDayAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryDayAt<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryDayAt {
            day: self.day.in_timezone(new_tz),
            hour: self.hour,
            minute: self.minute,
            second: self.second,
        }
    }
}

impl<TZ> Job for EveryDayAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        cyclic_time_to_sleep_at(
            now, (Some(self.hour), Some(self.minute), self.second),
            (self.day.step, now.day(), 24 * 60 * 60),
        )
    }

    fn timezone(&self) -> &Self::TZ { &self.day.tz }
}

#[derive(Debug, Clone)]
pub struct EveryWeekDay<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    weekday: Weekday,
    tz: TZ,
}

impl<TZ> EveryWeekDay<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn at(self, hour: u32, minute: u32, second: u32) -> EveryWeekDayAt<TZ> {
        EveryWeekDayAt { day: self, hour, minute, second }
    }

    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryWeekDay<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    { EveryWeekDay { weekday: self.weekday, tz: new_tz.clone() } }
}

impl<TZ> Job for EveryWeekDay<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        EveryWeekDayAt {
            day: self.clone(),
            hour: 00,
            minute: 00,
            second: 00,
        }.time_to_sleep_at(now)
    }

    fn timezone(&self) -> &Self::TZ { &self.tz }
}

#[derive(Debug, Clone)]
pub struct EveryWeekDayAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    day: EveryWeekDay<TZ>,
    hour: u32,
    minute: u32,
    second: u32,
}

impl<TZ> EveryWeekDayAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    pub fn in_timezone<NewTZ>(self, new_tz: &NewTZ) -> EveryWeekDayAt<NewTZ>
        where NewTZ: Clone + TimeZone + Send + Sync,
    {
        EveryWeekDayAt {
            day: self.day.in_timezone(new_tz),
            hour: self.hour,
            minute: self.minute,
            second: self.second,
        }
    }
}

impl<TZ> Job for EveryWeekDayAt<TZ>
    where TZ: Clone + TimeZone + Send + Sync,
{
    type TZ = TZ;

    fn time_to_sleep_at(&self, now: &DateTime<TZ>) -> Duration {
        let offset_nanos = {
            let nanos = self.second as i64 * 1_000_000_000 - (now.second() as i64 * 1_000_000_000 + now.nanosecond() as i64);
            let minutes = self.minute as i64 - now.minute() as i64;
            let hours = self.hour as i64 - now.hour() as i64;
            (hours * 60  + minutes) * 60 * 1_000_000_000 + nanos
        };

        let interval_nanos = {
            let skip_days =
                self.day.weekday.number_from_monday() as i32 - now.weekday().number_from_monday() as i32;

            if skip_days < 0 || (skip_days == 0 && offset_nanos <= 0) { skip_days + 7 } else { skip_days }
        } as i64 * 24 * 60 * 60 * 1_000_000_000;

        Duration::from_nanos((interval_nanos + offset_nanos) as u64)
    }

    fn timezone(&self) -> &Self::TZ { &self.day.tz }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{DateTime, Utc, NaiveDate, Weekday};
    use regex::Regex;

    use crate::{Job, every, every_week};


    #[test]
    fn every_second() {
        assert_eq!(
            every(30).seconds().in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 30, 10)).as_secs(),
            "20s".secs()
        );

        assert_eq!(
            every(30).seconds().in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 30, 50)).as_secs(),
            "10s".secs()
        );
    }

    #[test]
    fn every_minute() {
        assert_eq!(
            every(30).minutes().at(20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 30, 10)).as_secs(),
            "10s".secs()
        );

        assert_eq!(
            every(30).minutes().at(20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 20, 10)).as_secs(),
            "10m 10s".secs()
        );

        assert_eq!(
            every(30).minutes().at(20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 20, 50)).as_secs(),
            "9m 30s".secs()
        );

        assert_eq!(
            every(1).minutes().at(20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 20, 10)).as_secs(),
            "10s".secs()
        );

        assert_eq!(
            every(1).minutes().at(20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 20, 30)).as_secs(),
            "50s".secs()
        );

        assert_eq!(
            every(1).minutes().at(20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 21, 20)).as_secs(),
            "1m".secs()
        );
    }

    #[test]
    fn every_hour() {
        assert_eq!(
            every(3).hours().at(10, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 30, 10)).as_secs(),
            "1h 40m 10s".secs()
        );

        assert_eq!(
            every(3).hours().at(10, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(23, 20, 50)).as_secs(),
            "49m 30s".secs()
        );

        assert_eq!(
            every(3).hours().at(10, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 20, 50)).as_secs(),
            "1h 49m 30s".secs()
        );


        assert_eq!(
            every(3).hours().at(10, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(12, 09, 10)).as_secs(),
            "1m 10s".secs()
        );

        assert_eq!(
            every(1).hours().at(10, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(23, 05, 00)).as_secs(),
            "5m 20s".secs()
        );

        assert_eq!(
            every(1).hours().at(10, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(00, 10, 20)).as_secs(),
            "1h".secs()
        );
    }

    #[test]
    fn every_day() {
        assert_eq!(
            every(2).days().at(10, 20, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 30, 10)).as_secs(),
            "23h 50m 10s".secs()
        );

        assert_eq!(
            every(2).days().at(10, 20, 20).in_timezone(&Utc)
                .time_to_sleep_at(&dt(23, 20, 50)).as_secs(),
            "10h 59m 30s".secs()
        );

        assert_eq!(
            every(1).day().in_timezone(&Utc)
                .time_to_sleep_at(&dt(23, 20, 50)).as_secs(),
            "39m 10s".secs()
        );

        assert_eq!(
            every(1).day().at(10, 00, 00).in_timezone(&Utc)
                .time_to_sleep_at(&dt(00, 00, 00)).as_secs(),
            "10h".secs()
        );

        assert_eq!(
            every(1).day().at(10, 00, 00).in_timezone(&Utc)
                .time_to_sleep_at(&dt(11, 00, 00)).as_secs(),
            "23h".secs()
        );

        assert_eq!(
            every(1).day().at(10, 00, 00).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 00, 00)).as_secs(),
            "24h".secs()
        );


        assert_eq!(
            every(1).day().at(10, 10, 50).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 09, 50)).as_secs(),
            "1m".secs()
        );

        assert_eq!(
            every(5).day().at(10, 00, 00).in_timezone(&Utc)
                .time_to_sleep_at(&dt(11, 00, 00)).as_secs(),
            "1d 23h".secs()
        );
    }

    #[test]
    fn every_weekday() {
        assert_eq!(
            every_week(Weekday::Wed).in_timezone(&Utc)
                .time_to_sleep_at(&dt(00, 00, 01)).as_secs(),
            "6d 23h 59m 59s".secs()
        );

        assert_eq!(
            every_week(Weekday::Thu).at(10, 00, 00).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 00, 00)).as_secs(),
            "1d".secs()
        );

        assert_eq!(
            every_week(Weekday::Wed).at(10, 00, 00).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 00, 00)).as_secs(),
            "7d".secs()
        );

        assert_eq!(
            every_week(Weekday::Mon).at(10, 00, 00).in_timezone(&Utc)
                .time_to_sleep_at(&dt(10, 00, 00)).as_secs(),
            "5d".secs()
        );
    }


    trait ParseToSecs {
        fn secs(&self) -> u64;
    }

    impl ParseToSecs for str {
        fn secs(&self) -> u64 {
            let re = Regex::new(
                r#"(?:(\d)d)?\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?"#
            ).unwrap();
            let caps = re.captures(self).unwrap();

            let mut res = 0u64;
            caps.get(1).map(|days| {
                res += u64::from_str(days.as_str()).unwrap() * 24 * 60 * 60;
            });
            caps.get(2).map(|hours| {
                res += u64::from_str(hours.as_str()).unwrap() * 60 * 60;
            });
            caps.get(3).map(|minutes| {
                res += u64::from_str(minutes.as_str()).unwrap() * 60;
            });
            caps.get(4).map(|seconds| {
                res += u64::from_str(seconds.as_str()).unwrap();
            });

            res
        }
    }

    fn dt(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        let day = NaiveDate::from_ymd(1998, 12, 23);
        DateTime::from_utc(day.and_hms(hour, minute, second), Utc)
    }


}
