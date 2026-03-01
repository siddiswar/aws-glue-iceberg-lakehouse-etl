from __future__ import annotations

import types
from datetime import datetime, timezone

from src.lambdas.compute_yesterday_dt.app import lambda_handler


def _patch_datetime_now(monkeypatch, fixed_now_utc: datetime):
    """
    Patch datetime.now(tz) used in the module to return a deterministic time.
    """

    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now_utc.replace(tzinfo=None)
            return fixed_now_utc.astimezone(tz)

    import src.lambdas.compute_yesterday_dt.app as mod
    monkeypatch.setattr(mod, "datetime", FakeDateTime)


def test_returns_yesterday_date(monkeypatch):
    """
    2026-02-21 UTC -> still 2026-02-21 in London daytime
    """
    _patch_datetime_now(monkeypatch, datetime(2026, 2, 21, 10, 0, 0, tzinfo=timezone.utc))
    out = lambda_handler({}, types.SimpleNamespace())
    assert out == {"dt": "2026-02-20"}


def test_london_timezone_boundary_just_after_midnight(monkeypatch):
    """
    If it's just after midnight in London, yesterday should still be computed correctly.
    00:05 in London on 2026-02-21 is 00:05 UTC (winter time)
    """

    _patch_datetime_now(monkeypatch, datetime(2026, 2, 21, 0, 5, 0, tzinfo=timezone.utc))
    out = lambda_handler({}, types.SimpleNamespace())
    assert out["dt"] == "2026-02-20"


def test_dst_transition_day_does_not_break(monkeypatch):
    """
    DST start in UK is end of March. This test simply ensures no crash and correct date math.
    2026-03-29 01:30 UTC -> 02:30 Europe/London (after DST shift)
    """
    _patch_datetime_now(monkeypatch, datetime(2026, 3, 29, 1, 30, 0, tzinfo=timezone.utc))
    out = lambda_handler({}, types.SimpleNamespace())
    assert out["dt"] == "2026-03-28"
