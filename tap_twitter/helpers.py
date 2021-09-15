import datetime

import pytz
import singer


def date_to_rfc3339(date: str) -> str:
    """Converts date to rfc 3339"""
    d = singer.utils.strptime_to_utc(date)

    return d.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_bookmark_or_max_date(date: str, lookback_days=7) -> str:
    """If the bookmark date is older than max lookback days,
    returns the max date"""

    now = datetime.datetime.utcnow().astimezone(pytz.UTC)
    max_lookback_date = now - datetime.timedelta(days=lookback_days)

    if singer.utils.strptime_to_utc(date) < max_lookback_date:
        return (max_lookback_date + datetime.timedelta(seconds=30)).isoformat()
    else:
        return date
