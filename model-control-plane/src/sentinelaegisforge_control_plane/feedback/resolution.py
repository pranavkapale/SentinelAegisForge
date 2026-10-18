"""Pure revision-order, receipt-time AS_OF resolution; no model or filesystem access."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from sentinelaegisforge_control_plane.datasets.labels import Outcome, require_utc

from .contract import FeedbackError, FeedbackRecord, FeedbackStatus


class EffectiveState(StrEnum):
    FRAUD = "FRAUD"
    LEGIT = "LEGIT"
    PENDING = "PENDING"
    RETRACTED = "RETRACTED"
    NOT_YET_OBSERVED = "NOT_YET_OBSERVED"
    NO_CASE = "NO_CASE"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class SelectedRevision:
    record: FeedbackRecord
    effective_available_at: datetime


@dataclass(frozen=True)
class ResolvedFeedback:
    event_id: str
    state: EffectiveState
    selected: tuple[SelectedRevision, ...]
    case_count: int


def confirmed_projection(item: ResolvedFeedback) -> Outcome | None:
    """Only a current confirmation enters the binary evaluation projection."""
    if item.state is EffectiveState.FRAUD:
        return Outcome.FRAUD
    if item.state is EffectiveState.LEGIT:
        return Outcome.LEGIT
    return None


def resolve_feedback(
    records: tuple[FeedbackRecord, ...],
    as_of: datetime,
    event_ids: tuple[str, ...],
) -> dict[str, ResolvedFeedback]:
    """A revision is eligible only after every predecessor has been received."""
    try:
        cutoff = require_utc(as_of)
    except ValueError as error:
        raise FeedbackError(str(error)) from error
    cases: dict[tuple[str, str], list[FeedbackRecord]] = defaultdict(list)
    for record in records:
        cases[(record.provider_id, record.provider_case_id)].append(record)
    by_event: dict[str, list[SelectedRevision]] = defaultdict(list)
    known_case_counts: dict[str, int] = defaultdict(int)
    for case, history in sorted(cases.items()):
        ordered = sorted(history, key=lambda item: item.source_revision)
        if [item.source_revision for item in ordered] != list(range(1, len(ordered) + 1)):
            raise FeedbackError(f"Incomplete case history: {case}")
        if len({item.event_id for item in ordered}) != 1:
            raise FeedbackError(f"Case changes event identity: {case}")
        event_id = str(ordered[0].event_id)
        known_case_counts[event_id] += 1
        availability = ordered[0].received_at
        selected: SelectedRevision | None = None
        for record in ordered:
            availability = max(availability, record.received_at)
            if availability <= cutoff:
                selected = SelectedRevision(record, availability)
        if selected is not None:
            by_event[event_id].append(selected)
    result: dict[str, ResolvedFeedback] = {}
    for event_id in event_ids:
        if event_id in result:
            raise FeedbackError(f"Duplicate requested event_id: {event_id}")
        selected_cases = tuple(
            sorted(
                by_event.get(event_id, []),
                key=lambda item: (item.record.provider_id, item.record.provider_case_id),
            )
        )
        statuses = {item.record.status for item in selected_cases}
        if (
            FeedbackStatus.CONFIRMED_FRAUD in statuses
            and FeedbackStatus.CONFIRMED_LEGIT in statuses
        ):
            state = EffectiveState.AMBIGUOUS
        elif FeedbackStatus.CONFIRMED_FRAUD in statuses:
            state = EffectiveState.FRAUD
        elif FeedbackStatus.CONFIRMED_LEGIT in statuses:
            state = EffectiveState.LEGIT
        elif FeedbackStatus.RETRACTED in statuses:
            state = EffectiveState.RETRACTED
        elif FeedbackStatus.PENDING in statuses:
            state = EffectiveState.PENDING
        elif known_case_counts[event_id]:
            state = EffectiveState.NOT_YET_OBSERVED
        else:
            state = EffectiveState.NO_CASE
        result[event_id] = ResolvedFeedback(
            event_id, state, selected_cases, known_case_counts[event_id]
        )
    return result
