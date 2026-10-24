# Phase 27 local incremental verification

This is a controlled local research experiment, not a production capacity, latency, availability or fraud-quality assessment. The generated source is a 25-column `LOCAL_INCREMENTAL_FEATURE_FIXTURE`, not output of the Scala stateful feature pipeline. Detailed immutable evidence is under ignored `.local/phase27/`; it is not committed or backed up by this document.

## Identity and source progression

- Operational stream: `80d8d88dd23b353388ee6e299742767cac64f5162e7dab73a7fee49103c38b20`.
- Fixture Delta table: `300a6f49-1c5a-4d53-a908-9e9daa2cb8b5`; exact statistical schema fingerprint: `ee469e25981c926d20cd926d35c225175c1bc3d4c08c6ecf07c2d8364d9baea5`.
- Version 0 established the empty table. Separate later commits added 5 rows at v1, 12 at v2, 10 at v3 and 4 at v4. Each wave was discovered after its commit; they were not prebuilt as one historical scan.
- The fixed `features.ready` topic ID was `at-fjc4PSciLLeK-EGcBig` in cluster `4L6g3nShT-eMCtK--X86sw`. The dedicated group `sentinel-feature-ready-incremental-shadow-v1` was initialized at per-partition end offsets `1524/1401/1579`, fencing off older research records without altering their group offsets.
- The research-only Registry version was 1, candidate ID `b4fff4f1f74818165d04bae094b89f7c7b8dc7367d6e3dad10465b1a6160fadb`, model-tree SHA-256 `c8bdcb0f3d92b0fbd222a5a6bfce1a67d8f207abe26b00c60d86830e5a8fb6fa`.

| Delta version | Rows | Segment ID | Outbox ID | Kafka batch ID |
| --- | ---: | --- | --- | --- |
| 1 | 5 | `21aca223badfb70e8e569c817104e01a0395cb13ba2eb25c7a084c96ff063087` | `04468475e7eb57a7474f9eb23038ed13c7627b5b56fa213abeba59152a355d76` | `0810ecbcf01b16788e1b11b164a7bab5ba0c65722b69a318878b21ded872a777` |
| 2 | 12 | `fd50c115d614365a15d00c303d8dd729ce44141e37f0ecd2a9ba747917bace59` | `c15628bad265ae055653b30f38f5dc34819e6a28ad94de7755b072b1388f4531` | `fd47a99912ad8726e0f3068e99327703da317eedd8f37ffe03011651e9d08211` |
| 3 | 10 | `b72e1a6d66cef2727f032eb1e0adf4c02e094c04548b9664fadee0a5243d1bbf` | `2db7cbbe24fc78ad600d295bb8fa039c69802633992ad001b833e39094f48f6f` | `7d2f9f52bc79e2082825f33655bf5c113110139699f1e1a4e55e1fb57cc227c5` |
| 4 | 4 | `ef4e61b7d6e44935feff3c3c199bfb5fd2aef89ef02350037c4f8699ab2df6fe` | `fa27c3bebec4a9bb82c9a952a05fa7779deb40406ab6e9afed52bba3d856f802` | `b177b947ce0f1ecc4be5f55879906b3981572a57807e2afb25ea317e7841a926` |

## Backpressure and recovery observations

The experiment set the accepted-unprocessed limit to 20 and the per-tick inference limit to 10. Wave 2 was durably accepted while inference was intentionally paused, producing an observed inference backlog of 12. Preparing wave 3 raised proposed occupancy to 22, so the publication gate returned `PAUSED_BACKPRESSURE` without deleting its outbox or sending the batch. An injected worker interruption after three durable results left those three files intact; a restarted tick completed the remaining nine, the backlog returned to zero, and wave 3 could publish. No previous successful result was overwritten.

For the broker outage test, `make infra-down` stopped Kafka without removing its volume. Publication of the already prepared wave-3 batch failed with a broker timeout before its journal had an acknowledgement. `make infra-up` restored Kafka; the same batch then received 10/10 acknowledgements and was accepted. This observed failure was *before* acknowledgement, not proof that uncertain post-ack sends never duplicate. A separate injected receipt-before-offset-commit failure after three receipts resumed using the same batch and group, then sealed all ten receipts. The existing Phase 25 unit tests continue to cover uncertain acknowledgement and duplicate delivery mechanisms.

A separate temporary Delta unit fixture retained a required source version while work was pending, temporarily moved its version-1 log outside the table, observed a fail-closed `Unverifiable Delta version 1` with no inference result, restored the log and resolved the work. No historical Phase 14 Delta log or user-owned source was changed or vacuumed.

## Reconciliation and measurements

The final read-only reconciliation reported exact source/publication/receipt/result ready-event ID equality: **31 unique source events, 31 physical Kafka records, 31 accepted notifications, 31 scored durable results, 0 physical duplicates, 0 pending, 0 failed and 0 out of scope**. Independent offline scoring of the same pinned fixture rows matched **31/31** probabilities with **0 mismatches**. The final content-addressed local report is `.local/phase27/streams/80d8d88dd23b353388ee6e299742767cac64f5162e7dab73a7fee49103c38b20/reports/499feee88764c2bd8e4fd9b8cdca166dbbad0c435f7a9a002efb0166cbf5b26a/`.

Its recorded monotonic samples are: one v4 Delta write `0.057904s`, one v4 discovery/outbox call `0.427955s`, one v4 Kafka publication `0.134232s`, one v4 acceptance `3.862129s`, four successful inference ticks `1.273189s`, `1.472986s`, `1.068407s`, `0.855430s`, and four corresponding model loads `0.118441s`, `0.116103s`, `0.110118s`, `0.127269s`. The interrupted worker attempt also loaded a model but did not yield a completed tick timing sample. These few local observations do not define a percentile, benchmark, SLO or capacity target. Cross-process elapsed stages are not combined into a causal end-to-end latency.

Historical Phase 26 evidence remains a separate 1,500-result/1,000-scored reference with 1,000/1,000 Phase 19 probability parity; the new fixture does not extend that ledger or establish original online feature availability.
