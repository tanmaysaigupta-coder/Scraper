from datetime import UTC, datetime, timedelta

from src.crawler.dates import FreshnessState, parse_date, within_window

NOW = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)


def test_relative_dates():
    assert parse_date("2 hours ago", now=NOW) == NOW - timedelta(hours=2)
    assert parse_date("30 minutes ago", now=NOW) == NOW - timedelta(minutes=30)
    assert parse_date("just now", now=NOW) == NOW
    assert parse_date("Yesterday 4pm", now=NOW).date() == (NOW - timedelta(days=1)).date()


def test_iso_and_rfc_dates():
    assert parse_date("2026-08-29T09:00:00Z", now=NOW) == datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    assert parse_date("Fri, 29 Aug 2026 06:00:00 GMT", now=NOW).hour == 6


def test_missing_date_returns_none():
    assert parse_date("", now=NOW) is None
    assert parse_date(None, now=NOW) is None
    assert parse_date("garbage not a date", now=NOW) is None


def test_within_window():
    assert within_window(NOW - timedelta(hours=5), hours=24, now=NOW)
    assert not within_window(NOW - timedelta(hours=30), hours=24, now=NOW)
    assert not within_window(None, hours=24, now=NOW)


def test_freshness_state_heuristic(tmp_path):
    st = FreshnessState(tmp_path / "state.json")
    assert st.is_new("SourceX", NOW)          # never seen -> new
    st.observe("SourceX", NOW)
    assert not st.is_new("SourceX", NOW - timedelta(hours=1))
    assert st.is_new("SourceX", NOW + timedelta(hours=1))
    st.save()
    assert FreshnessState(tmp_path / "state.json").high_water("SourceX") == NOW
