import calendar
from datetime import date, timedelta

from holidays import NL  # type: ignore[attr-defined]

from src.utils.logging_util import get_logger

logger = get_logger()


def get_last_business_day(year: int, month: int) -> date:
    dutch_holidays = list(NL(years=year).keys()) + list(
        NL(years=year, categories="optional").keys()
    )
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    last_7days = [(last_day - timedelta(days=d)) for d in range(7)]
    max_day_business = 5
    return max(
        [
            d
            for d in last_7days
            if d.weekday() < max_day_business and d not in dutch_holidays
        ]
    )


def get_first_business_day(year: int, month: int) -> date:
    december = 12
    year_ = year if month < december else year + 1
    month_ = month + 1 if month < december else 1
    dutch_holidays = list(NL(years=year_).keys()) + list(
        NL(years=year_, categories="optional").keys()
    )
    first_day = date(year_, month_, 1)
    first_7days = [(first_day + timedelta(days=d)) for d in range(7)]
    max_day_business = 5
    return min(
        [
            d
            for d in first_7days
            if d.weekday() < max_day_business and d not in dutch_holidays
        ]
    )


def get_last_day_of_week(year: int, month: int, day_of_week: int) -> date:
    dutch_holidays = list(NL(years=year).keys()) + list(
        NL(years=year, categories="optional").keys()
    )
    return max(
        [
            date(year, month, w[day_of_week])
            for w in calendar.monthcalendar(year, month)
            if (
                w[day_of_week] != 0
                and date(year, month, w[day_of_week]) not in dutch_holidays
            )
        ]
    )


def derive_snapshot_date(year: int, month: int, schedule: str) -> str:
    """Supported schedules:
    - M-1: Last working day of the month
    - M-F: Last Friday of the month
    - M+1: First working day of the month
    - M-T: Last Tuesday of the month
    """
    if schedule == "M-1":
        snapshot_date = get_last_business_day(year, month)
    elif schedule == "M-F":
        snapshot_date = get_last_day_of_week(year, month, 4)
    elif schedule == "M+1":
        snapshot_date = get_first_business_day(year, month)
    elif schedule == "M-T":
        snapshot_date = get_last_day_of_week(year, month, 1)
    else:
        logger.error(f"Unsupported schedule: {schedule}")
        return ""

    logger.info(
        f"Derived snapshotdate for {month}-{year}, schedule {schedule}: "
        f"{snapshot_date}"
    )
    return snapshot_date.strftime("%Y-%m-%d")
