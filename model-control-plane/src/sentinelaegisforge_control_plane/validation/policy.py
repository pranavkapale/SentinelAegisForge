"""Pure, versioned promotion-readiness evidence policy."""

import hashlib
from dataclasses import asdict, dataclass
from enum import StrEnum

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

POLICY_VERSION = "model-validation-policy-v1"
LOGIC_VERSION = "promotion-readiness-assessment-v1.1"


class EvidenceState(StrEnum):
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"
    NOT_ESTABLISHED = "NOT_ESTABLISHED"


@dataclass(frozen=True)
class Requirement:
    identifier: str
    meaning: str
    mandatory: bool = True


REQUIREMENTS = (
    Requirement("VAL001_ARTIFACT_INTEGRITY", "Model presence, runtime and content integrity"),
    Requirement("VAL002_MODEL_RECONSTRUCTION", "Archived prediction reconstruction equivalence"),
    Requirement("VAL003_DATASET_PROVENANCE", "Verified dataset, training and tracking lineage"),
    Requirement("VAL004_REAL_WORLD_LABEL_QUALITY", "Independent real fraud outcomes"),
    Requirement("VAL005_TEMPORAL_GENERALIZATION", "Representative out-of-time performance"),
    Requirement("VAL006_MODEL_DISCRIMINATION", "Production-relevant fraud discrimination"),
    Requirement("VAL007_PROBABILITY_CALIBRATION", "Validated probability calibration"),
    Requirement("VAL008_SEGMENT_ROBUSTNESS", "Population and currency robustness"),
    Requirement("VAL009_OPERATIONAL_VALIDATION", "Serving, failure and monitoring evidence"),
    Requirement("VAL010_BUSINESS_GOVERNANCE", "Explicit model-risk approval and accountability"),
)


@dataclass(frozen=True)
class ValidationPolicy:
    version: str = POLICY_VERSION
    requirements: tuple[Requirement, ...] = REQUIREMENTS
    logic_version: str = LOGIC_VERSION

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(
            canonical_json(
                {
                    "version": self.version,
                    "requirements": [asdict(item) for item in self.requirements],
                    "logic_version": self.logic_version,
                    "states": {
                        "VERIFIED": "authoritative evidence supports requirement",
                        "FAILED": "authoritative evidence demonstrates violation",
                        "NOT_ESTABLISHED": "evidence absent, insufficient or unverifiable",
                    },
                    "blocking": "every mandatory FAILED or NOT_ESTABLISHED requirement blocks",
                    "outcome": "NOT_ELIGIBLE_FOR_PROMOTION; production_eligible=false",
                }
            )
        ).hexdigest()


@dataclass(frozen=True)
class EvidenceFact:
    state: EvidenceState
    explanation: str
    references: tuple[str, ...] = ()


@dataclass(frozen=True)
class VerifiedFacts:
    """Typed collector output. Live collection never accepts user-supplied fact JSON."""

    artifact_integrity: EvidenceFact
    model_reconstruction: EvidenceFact
    dataset_provenance: EvidenceFact
    real_world_label_quality: EvidenceFact
    temporal_generalization: EvidenceFact
    model_discrimination: EvidenceFact
    probability_calibration: EvidenceFact
    segment_robustness: EvidenceFact
    operational_validation: EvidenceFact
    business_governance: EvidenceFact


FACT_FIELDS = (
    "artifact_integrity",
    "model_reconstruction",
    "dataset_provenance",
    "real_world_label_quality",
    "temporal_generalization",
    "model_discrimination",
    "probability_calibration",
    "segment_robustness",
    "operational_validation",
    "business_governance",
)


def evaluate(
    facts: VerifiedFacts, policy: ValidationPolicy = ValidationPolicy()
) -> dict[str, object]:
    """Deterministic evaluation. No I/O, runtime clock or approval path."""
    if not isinstance(facts, VerifiedFacts):
        raise TypeError("Expected typed VerifiedFacts, not untrusted evidence fields")
    if (
        not policy.version.strip()
        or tuple(item.identifier for item in policy.requirements)
        != tuple(item.identifier for item in REQUIREMENTS)
        or not all(item.mandatory for item in policy.requirements)
    ):
        raise ValueError("Policy requirements must retain the ten mandatory typed categories")
    results: list[dict[str, object]] = []
    blocking: list[str] = []
    missing: list[str] = []
    for requirement, field in zip(policy.requirements, FACT_FIELDS, strict=True):
        fact = getattr(facts, field)
        if not isinstance(fact, EvidenceFact) or not isinstance(fact.state, EvidenceState):
            raise TypeError(f"Invalid typed evidence: {field}")
        if not fact.explanation:
            raise ValueError(f"Evidence explanation is missing: {field}")
        blocks = requirement.mandatory and fact.state != EvidenceState.VERIFIED
        results.append(
            {
                "requirement_id": requirement.identifier,
                "meaning": requirement.meaning,
                "mandatory": requirement.mandatory,
                "status": fact.state.value,
                "explanation": fact.explanation,
                "evidence_references": list(fact.references),
                "blocks_promotion": blocks,
            }
        )
        if blocks:
            blocking.append(f"{requirement.identifier}: {fact.state.value} — {fact.explanation}")
        if fact.state == EvidenceState.NOT_ESTABLISHED:
            missing.append(requirement.identifier)
    if not blocking:
        blocking.append("Phase 18 assessment has no production approval authority")
    return {
        "requirements": results,
        "technical_integrity_verified": all(
            item["status"] == EvidenceState.VERIFIED.value for item in results[:3]
        ),
        "blocking_reasons": blocking,
        "missing_evidence": missing,
        "promotion_readiness": "NOT_ELIGIBLE_FOR_PROMOTION",
        "production_eligible": False,
    }
