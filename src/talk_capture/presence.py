"""Build join/leave presence annotations for a call transcript.

Pure functions, no I/O. Turns raw per-session presence intervals (unix ts)
into chronological (offset_sec, name, kind) events relative to call start.
"""
from __future__ import annotations

# Adjacent presence intervals of one person split by less than this gap are
# a technical reconnect (signaling disconnect/rejoin), not a real leave+join.
_RECONNECT_MERGE_GAP = 30.0  # seconds

# Speaker-name placeholders generated when attribution failed. They are still
# shown as presence events (product decision), but a real name is preferred
# when grouping a person's sessions.
_PLACEHOLDER_PREFIXES = ("Спикер (", "Участник (")


def _is_placeholder(name: str) -> bool:
    return bool(name) and name.startswith(_PLACEHOLDER_PREFIXES)


def _merge_intervals(
    intervals: list[tuple[float, float | None]],
    merge_gap_s: float,
) -> list[tuple[float, float | None]]:
    """Sort by start; merge adjacent intervals whose gap < merge_gap_s.

    An open interval (end is None) cannot be bridged to a later one — it runs
    to the end of the call — so it ends the merge chain.
    """
    ordered = sorted(intervals, key=lambda iv: iv[0])
    merged: list[list] = []
    for start, end in ordered:
        prev_end = merged[-1][1] if merged else None
        if merged and prev_end is not None and start - prev_end < merge_gap_s:
            if end is None or end > prev_end:
                merged[-1][1] = end
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def build_presence_events(
    raw: list[tuple[str, str, list[tuple[float, float | None]]]],
    call_start_ts: float,
    call_end_ts: float,
    merge_gap_s: float = _RECONNECT_MERGE_GAP,
) -> list[tuple[float, str, str]]:
    """Turn raw per-session presence into chronological join/leave events.

    `raw` is one (name, group_key, intervals) entry per session_id.
    group_key is the user_id (or "sid:<session_id>" for anonymous guests):
    entries sharing a key are the same person, so their intervals are merged
    — this collapses reconnects that signaling re-issued under a new SID.

    Returns (offset_sec, name, kind) with kind in {"join", "leave"}, sorted
    by offset. Offsets are relative to call_start; leave is clamped to
    call_end (an interval left open, or closed after call_end during the
    post-call disconnect, becomes a leave exactly at call_end).
    """
    # 1. Group intervals by person; pick the best name per person.
    grouped: dict[str, list[tuple[float, float | None]]] = {}
    names: dict[str, str] = {}
    for name, key, intervals in raw:
        if not intervals:
            continue
        grouped.setdefault(key, []).extend(intervals)
        cur = names.get(key)
        if cur is None or (_is_placeholder(cur) and not _is_placeholder(name)):
            names[key] = name

    # 2. Per person: merge reconnects, expand to join/leave, clamp + offset.
    events: list[tuple[float, str, str]] = []
    for key, intervals in grouped.items():
        name = names.get(key, "")
        for start, end in _merge_intervals(intervals, merge_gap_s):
            join_off = max(0.0, start - call_start_ts)
            end_ts = end if end is not None else call_end_ts
            leave_off = max(0.0, min(end_ts, call_end_ts) - call_start_ts)
            events.append((join_off, name, "join"))
            events.append((leave_off, name, "leave"))

    events.sort(key=lambda e: e[0])
    return events
