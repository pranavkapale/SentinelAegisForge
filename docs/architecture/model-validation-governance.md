# Model validation governance (Phase 18)

```text
Exact research-only Registry version
    -> deep Phase 12–17 source inspection
    -> typed verified facts
    -> model-validation-policy-v1 pure evaluation
    -> immutable .local/ml/model-validations/<assessment_id>/ dossier
    -> NOT_ELIGIBLE_FOR_PROMOTION
    X  production approval / deployment (not implemented)
```

Technical artifact correctness is not training reproducibility; reproducibility is not experiment traceability; traceability is not Registry catalog status. None of these establishes statistical validity on representative fraud outcomes, operational readiness, or accountable business approval. Each has a distinct evidence requirement and none is inferred from another.

The dossier is an assessment, not approval. It does not train, score, deploy, alter Registry tags, or copy raw labels/predictions. `assessment.json` records ten ordered requirements and exact authoritative Phase 13 validation/test metrics. When the verified Phase 15 provenance explicitly names a Phase 14 corpus, inspection rechecks the existing corpus plan/private truth/label hashes and requires the labels hash to match the Phase 12 snapshot; it does not infer a corpus ID from a directory name. The report states that synthetic scenario truth is distinct from Phase 11 CLEAR/REVIEW decisions. `evidence_manifest.json` references upstream artifacts and their verified hashes; `validation_report.md` explains proven mechanics and blockers. The canonical assessment ID hashes the validation/logic versions, policy fingerprint, exact Registry backend/name/version, candidate/model content, training/snapshot lineage and source artifact hashes. It excludes clock time, hostnames and random values. Existing directories are checked byte-for-byte, never refreshed or repaired.

`VERIFIED` requires inspected authoritative evidence; `FAILED` means authoritative evidence demonstrates violation; `NOT_ESTABLISHED` means necessary evidence is missing or insufficient. Corrupt source bytes fail inspection before a dossier is published, rather than being mistaken for a routine evidence gap. No arbitrary operator JSON can self-attest a requirement. The v1 policy marks the first three technical requirements VERIFIED after deep inspection. The ingestion-time split and support mechanics are independently VERIFIED, including delayed-label exclusions. Representative temporal generalization remains NOT_ESTABLISHED because the data is synthetic. Real-world label quality, production discrimination, calibration, segment robustness, serving/failure/monitoring behavior and business approval remain NOT_ESTABLISHED.

The reported validation/test Average Precision, ROC AUC, precision/recall/F1 at 0.5, confusion counts and class support come unchanged from verified Phase 13 `metrics.json`; they are not recomputed or converted into a promotion threshold. Average Precision may be read alongside class prevalence descriptively. **Synthetic Scenario Evaluation — Not Production Fraud Performance.** There is no evidence that the current candidate is fraud-calibrated or production-ready.

Run `make assess-registered-model REGISTERED_MODEL_VERSION=1` with the existing local MLflow SQLite store, then inspect by explicit `MODEL_ASSESSMENT_PATH`. `make compare-model-assessments MODEL_ASSESSMENT_PATHS="<path1> <path2>"` provides a read-only summary without winner selection. These operations require no tracking server, Docker or new dependency. Read-only inspection re-verifies upstream sources; a missing or changed source is reported, never silently rewritten in the original dossier. Hashes are integrity checks, not signatures against privileged evidence replacement. The current policy can only return `NOT_ELIGIBLE_FOR_PROMOTION`, `production_eligible=false`; approval, serving and promotion are outside Phase 18.
