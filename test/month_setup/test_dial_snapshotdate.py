import datetime

import pytest

from src.month_setup.dial_derive_snapshotdate import (
    derive_snapshot_date,
    get_first_business_day,
    get_last_business_day,
    get_last_day_of_week,
)


@pytest.mark.parametrize(
    ("month", "year", "schedule", "expected_output", "expected_logging"),
    [
        # M-1
        (
            8,
            2024,
            "M-1",
            "2024-08-30",
            "Derived snapshotdate for 8-2024, schedule M-1: 2024-08-30",
        ),
        # M+1
        (
            8,
            2024,
            "M+1",
            "2024-09-02",
            "Derived snapshotdate for 8-2024, schedule M+1: 2024-09-02",
        ),
        # M-F
        (
            8,
            2024,
            "M-F",
            "2024-08-30",
            "Derived snapshotdate for 8-2024, schedule M-F: 2024-08-30",
        ),
        # M-T
        (
            8,
            2024,
            "M-T",
            "2024-08-27",
            "Derived snapshotdate for 8-2024, schedule M-T: 2024-08-27",
        ),
        # M+1 over next year (checking new years day off also)
        (
            12,
            2024,
            "M+1",
            "2025-01-02",
            "Derived snapshotdate for 12-2024, schedule M+1: 2025-01-02",
        ),
        # Unhappy scenario, invalid schedule
        (
            12,
            2024,
            "M+2",
            "",
            "Unsupported schedule: M+2",
        ),
    ],
)
def test_snapshot_date(
    month, year, schedule, expected_output, expected_logging, caplog
):
    assert derive_snapshot_date(year, month, schedule) == expected_output
    assert caplog.messages[0] == expected_logging


@pytest.mark.parametrize(
    ("month", "year", "expected_output"),
    [
        (
            8,
            2024,
            datetime.date(2024, 9, 2),
        ),
        (
            12,
            2024,
            datetime.date(2025, 1, 2),
        ),
    ],
)
def test_get_first_business_day(month, year, expected_output):
    assert get_first_business_day(year, month) == expected_output


@pytest.mark.parametrize(
    ("month", "year", "expected_output"),
    [
        (
            8,
            2024,
            datetime.date(2024, 8, 30),
        ),
        (
            9,
            2024,
            datetime.date(2024, 9, 30),
        ),
    ],
)
def test_get_last_business_day(month, year, expected_output):
    assert get_last_business_day(year, month) == expected_output


@pytest.mark.parametrize(
    ("month", "year", "day_of_week", "expected_output"),
    [
        # Last Friday
        (
            8,
            2024,
            4,
            datetime.date(2024, 8, 30),
        ),
        # Last Tuesday
        (
            8,
            2024,
            1,
            datetime.date(2024, 8, 27),
        ),
    ],
)
def test_get_last_day_of_week(month, year, day_of_week, expected_output):
    assert get_last_day_of_week(year, month, day_of_week) == expected_output
