# Research-only model Registry

```text
Phase 16 technically verified local candidate
  -> candidate integrity and Phase 12/13/15 provenance inspection
  -> typed research-registration eligibility
  -> MLflow 3.17.0 Model Registry (existing local SQLite backend)
  -> exact-version metadata + local source read-back
  -> immutable registration receipt
  -> REGISTERED_RESEARCH_ONLY
  -X-> production promotion (not implemented)
```

`candidate_id` is Phase 16's deterministic semantic identity. `source_tracking_run_id` is the Phase 15 MLflow experiment address; it tracked Phase 13 evidence but did **not** produce the later binary. `registered_model_name + model_version` is allocated by MLflow and is neither of those IDs. Registration does not retrain, change metrics or mutate the Phase 15 run.

The registry URI is the explicit Phase 15 `sqlite:///` URI at `.local/mlflow/tracking.db`; no HTTP server is needed. Model versions point to the existing immutable Phase 16 `file://.../model` directory. The Registry does not copy or back up bytes: keeping that local tree and its authoritative training/snapshot/tracking sources available is necessary for inspection. Version tags record candidate/source/dataset/input/hash identities and versioned governance fields. A separate receipt under ignored `.local/mlflow/registration-receipts/` records the verified registry identity and source hash. Tags and SHA-256 hashes are not cryptographic approvals or access control.

The only governance state is `REGISTERED_RESEARCH_ONLY`; incomplete creation uses `REGISTRATION_INCOMPLETE`. `registration_eligible=true` means the verified synthetic candidate can be cataloged; `production_eligible=false` remains mandatory. Synthetic scenario labels, below-baseline AP/ROC AUC, absent independent fraud outcomes, no calibration, no shadow/canary evidence, no business approval and no serving contract preclude a production-quality claim. MLflow's `READY` denotes registry creation only.

Registration verifies the candidate before mutation, serializes cooperating local writers with `flock`, searches all version pages, creates a version with `run_id=None`, reads source bytes back, marks it complete, verifies exact tags/source/version and publishes a receipt. A repeat returns the same numeric version and unchanged receipt. A duplicate claim, changed tag/source, or incomplete claim fails instead of overwriting or creating a replacement. External MLflow writers may bypass the local lock; later inspection detects conflicts but no global linearizability is claimed. An interrupted receipt may remain incomplete and must be investigated manually.

`inspect-registered-candidate` takes an explicit version and verifies metadata, source hashes, candidate contract and authoritative lineage without deserializing. `verify-registered-model-artifact` additionally uses Phase 16's hash/runtime/static-skops-allowlist trusted loader and repeats archived prediction-equivalence checks; a controlled offline probability is reported. `compare-registered-candidates` is read-only and warns on different datasets or split periods. Neither command resolves `latest`, chooses a winner, assigns an alias or serves inference. Artifact integrity, technical validity, catalog status, model quality and deployment approval are distinct boundaries.
