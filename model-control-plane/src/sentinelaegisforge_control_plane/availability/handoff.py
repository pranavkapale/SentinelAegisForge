"""Bounded, restartable, non-enforcing local delivery with durable logical receipts."""

import os
import tempfile
from pathlib import Path
from typing import cast
from uuid import uuid4

from .contract import (
    MAX_READY_EVENTS,
    READY_VERSION,
    RECEIPT_VERSION,
    AvailabilityError,
    digest,
    read_object,
)
from .outbox import canonical, inspect_outbox


def _atomic_new(path: Path, raw: bytes) -> bool:
    """Publish one file by exclusive link; partial temp files stay detectable."""
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=".pending-", delete=False
    ) as temporary:
        temporary.write(raw)
        temporary.flush()
        os.fsync(temporary.fileno())
        pending = Path(temporary.name)
    try:
        try:
            os.link(pending, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return True
        except FileExistsError:
            return False
    finally:
        pending.unlink()


class LocalResearchReceiver:
    """Records source-derived identities only; it has no model or payment side effect."""

    def __init__(self, outbox: Path, root: Path) -> None:
        if outbox.is_symlink():
            raise AvailabilityError("Outbox path must not be a symlink")
        self.outbox_path = outbox.resolve()
        self.outbox_manifest, ready = inspect_outbox(self.outbox_path)
        self.expected = {cast(str, event["ready_event_id"]): event for event in ready}
        self.ordered = ready
        self.root = root.resolve()
        if not self.root.exists():
            self.root.mkdir(parents=True)
            (self.root / "receipts").mkdir()
            (self.root / "attempts").mkdir()
            (self.root / "receiver_manifest.json").write_bytes(
                canonical(
                    {
                        "receipt_contract_version": RECEIPT_VERSION,
                        "outbox_id": self.outbox_manifest["outbox_id"],
                    }
                )
            )
        self.inspect()

    def _receipt(self, event: dict[str, object]) -> dict[str, object]:
        return {
            "receipt_contract_version": RECEIPT_VERSION,
            "receipt_id": digest([RECEIPT_VERSION, self.outbox_manifest["outbox_id"], event]),
            "outbox_id": self.outbox_manifest["outbox_id"],
            "ready_event_id": event["ready_event_id"],
            "event_id": event["event_id"],
            "envelope_fingerprint": digest(event),
        }

    def accept(self, event: dict[str, object], *, fail_before_receipt: bool = False) -> bool:
        ready_id = event.get("ready_event_id")
        if (
            not isinstance(ready_id, str)
            or event.get("availability_contract_version") != READY_VERSION
            or self.expected.get(ready_id) != event
        ):
            raise AvailabilityError("Unknown or conflicting ready-event envelope")
        if fail_before_receipt:
            raise AvailabilityError("Injected failure before durable receipt")
        receipt = self._receipt(event)
        path = self.root / "receipts" / f"{ready_id}.json"
        created = _atomic_new(path, canonical(receipt))
        if not created and read_object(path) != receipt:
            raise AvailabilityError("Conflicting reuse of ready-event receipt identity")
        return created

    def _attempt(self, event: dict[str, object], status: str) -> None:
        # Operational attempt identity, never semantic readiness identity.
        attempt_id = str(uuid4())
        path = self.root / "attempts" / f"{attempt_id}.json"
        attempt = {
            "attempt_id": attempt_id,
            "outbox_id": self.outbox_manifest["outbox_id"],
            "ready_event_id": event["ready_event_id"],
            "outcome": status,
        }
        if not _atomic_new(path, canonical(attempt)):
            raise AvailabilityError("Attempt identity collision")

    def deliver(self, event: dict[str, object], *, fail_before_receipt: bool = False) -> bool:
        try:
            created = self.accept(event, fail_before_receipt=fail_before_receipt)
        except AvailabilityError:
            if event.get("ready_event_id") in self.expected:
                self._attempt(event, "FAILED")
            raise
        self._attempt(event, "ACCEPTED" if created else "DUPLICATE")
        return created

    def inspect(self) -> dict[str, object]:
        current_manifest, current_ready = inspect_outbox(self.outbox_path)
        if current_manifest != self.outbox_manifest or current_ready != self.ordered:
            raise AvailabilityError("Verified outbox changed during local handoff")
        if (
            self.root.is_symlink()
            or not self.root.is_dir()
            or {item.name for item in self.root.iterdir()}
            != {"receiver_manifest.json", "receipts", "attempts"}
        ):
            raise AvailabilityError("Incomplete or unexpected local receiver directory")
        metadata = read_object(self.root / "receiver_manifest.json")
        if metadata != {
            "receipt_contract_version": RECEIPT_VERSION,
            "outbox_id": self.outbox_manifest["outbox_id"],
        }:
            raise AvailabilityError("Receiver is bound to a different outbox")
        receipts_path, attempts_path = self.root / "receipts", self.root / "attempts"
        if any(path.is_symlink() or not path.is_dir() for path in (receipts_path, attempts_path)):
            raise AvailabilityError("Receiver receipt/attempt directories are invalid")
        receipts: dict[str, dict[str, object]] = {}
        for path in receipts_path.iterdir():
            if path.is_symlink() or not path.is_file() or path.suffix != ".json":
                raise AvailabilityError(f"Incomplete receipt artifact: {path}")
            ready_id = path.stem
            event = self.expected.get(ready_id)
            if event is None or read_object(path) != self._receipt(event):
                raise AvailabilityError(f"Receipt conflicts with verified outbox: {path}")
            receipts[ready_id] = self._receipt(event)
        counts = {"ACCEPTED": 0, "DUPLICATE": 0, "FAILED": 0}
        for path in attempts_path.iterdir():
            if path.is_symlink() or not path.is_file() or path.suffix != ".json":
                raise AvailabilityError(f"Incomplete attempt artifact: {path}")
            attempt = read_object(path)
            outcome = attempt.get("outcome")
            if (
                path.stem != attempt.get("attempt_id")
                or attempt.get("outbox_id") != self.outbox_manifest["outbox_id"]
                or attempt.get("ready_event_id") not in self.expected
                or outcome not in counts
            ):
                raise AvailabilityError(f"Invalid delivery-attempt artifact: {path}")
            if outcome in {"ACCEPTED", "DUPLICATE"} and attempt["ready_event_id"] not in receipts:
                raise AvailabilityError(f"Acknowledged attempt has no durable receipt: {path}")
            counts[cast(str, outcome)] += 1
        fully_acknowledged: int | None = None
        for version in range(
            cast(int, self.outbox_manifest["start_delta_version"]),
            cast(int, self.outbox_manifest["end_delta_version"]) + 1,
        ):
            segment_ids = {
                cast(str, event["ready_event_id"])
                for event in self.ordered
                if event["first_visible_delta_version"] == version
            }
            if segment_ids <= receipts.keys():
                fully_acknowledged = version
            else:
                break
        return {
            "outbox_id": self.outbox_manifest["outbox_id"],
            "last_fully_examined_delta_version": self.outbox_manifest["end_delta_version"],
            "last_fully_acknowledged_delta_version": fully_acknowledged,
            "total_unique_ready_events": len(self.ordered),
            "accepted_receipts": len(receipts),
            "accepted_attempts": counts["ACCEPTED"],
            "duplicate_deliveries": counts["DUPLICATE"],
            "failed_deliveries": counts["FAILED"],
            "outstanding_events": len(self.ordered) - len(receipts),
            "receipt_event_ids_fingerprint": digest(sorted(receipts)),
        }

    def handoff(
        self,
        maximum: int,
        *,
        replay: bool = False,
        fail_on_ready_event_id: str | None = None,
    ) -> dict[str, object]:
        if type(maximum) is not int or not 1 <= maximum <= MAX_READY_EVENTS:
            raise AvailabilityError(f"Handoff maximum must be in [1,{MAX_READY_EVENTS}]")
        self.inspect()
        processed = 0
        for event in self.ordered:
            if processed >= maximum:
                break
            ready_id = cast(str, event["ready_event_id"])
            if not replay and (self.root / "receipts" / f"{ready_id}.json").exists():
                continue
            processed += 1
            if ready_id == fail_on_ready_event_id:
                try:
                    self.deliver(event, fail_before_receipt=True)
                except AvailabilityError:
                    break
            else:
                self.deliver(event)
        return self.inspect()

    def verify_complete(self) -> dict[str, object]:
        summary = self.inspect()
        receipt_ids = {path.stem for path in (self.root / "receipts").iterdir()}
        if receipt_ids != set(self.expected) or summary["outstanding_events"] != 0:
            raise AvailabilityError("Outbox-to-receiver ready-event ID reconciliation incomplete")
        return summary
