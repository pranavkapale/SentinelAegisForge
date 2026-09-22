# ADR-006: Kafka transaction partition key

## Status

Accepted

## Context

Future Module A processing will maintain customer-centric behavioral state, including transaction counts, amount windows, unique merchants and devices, previous transactions, and rolling statistics. The key contract for `transactions.raw` must align with that state model before a producer is introduced.

Kafka ordering is scoped to a partition, not to a topic or the platform globally. A stable business key can give future consumers useful locality and ordering for one customer's events under an unchanged partition topology.

## Decision

Use `TransactionEvent.customerId` as the Kafka record key for `transactions.raw`, encoded directly as a UTF-8 string. Do not manually hash it, include a partition number in the event, or introduce a custom partitioner. A future producer should use Kafka's normal keyed partition selection unless measured requirements justify a different approach.

This ADR establishes the logical contract; the bounded producer introduced later implements it without changing the key semantics.

## Alternatives considered

- **`transaction_id`:** likely distributes well when transaction identifiers are uniform, but does not naturally colocate events for the same customer and is a poor match for customer behavioral state.
- **`event_id`:** provides high-cardinality distribution, but deliberately disperses customer history.
- **`merchant_id`:** supports merchant-centric processing, but does not align with the primary customer state model and can create severe hot keys for large merchants.
- **No key:** is operationally simple and lets the producer distribute records, but provides no stable customer locality for future keyed state.

Each alternative serves a different access pattern; none is universally inferior.

## Trade-offs

Customer keys normally route one customer's events to one partition while the partition topology is stable, giving future consumers a useful per-customer, partition-scoped ordering property. They do not provide global ordering.

Highly active customers or reused synthetic customer identities can create skew. Future load testing must measure records per partition, processing rate per partition, lag per partition, and state-size distribution. Salting is not introduced because it would weaken straightforward customer ordering and state locality without an observed problem.

## Consequences

Future producers must encode the validated customer identifier as the UTF-8 record key. Consumers may design customer-keyed state around partition-scoped ordering, while still defining replay and duplicate-handling boundaries separately.

The current three-partition topic is local test configuration, not production sizing. With ordinary key-based partitioning, increasing the partition count can remap a customer's subsequent records to another partition. Before production partition expansion, the project must evaluate ordering implications, state migration, replay boundaries, and rollout strategy. This ADR does not solve those future operational concerns.
