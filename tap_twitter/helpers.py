import singer


def date_to_rfc3339(date: str) -> str:
    """Converts date to rfc 3339"""
    d = singer.utils.strptime_to_utc(date)

    return d.strftime('%Y-%m-%dT%H:%M:%SZ')
