# Transaction event v1

## Purpose

`TransactionEvent` represents a transaction occurrence whose version 1 domain fields have passed local, deterministic validation. It is the first trusted input boundary for future transaction processing; it is not a transport payload, Kafka record, fraud decision, or persisted schema.

## Fields

- `event_id`: identity of this business-event occurrence; `java.util.UUID`; required and parseable as a UUID.
- `transaction_id`: identity of the business transaction being described; `String`; required and non-blank.
- `customer_id`: customer reference; `String`; required and non-blank.
- `merchant_id`: merchant reference; `String`; required and non-blank.
- `event_time`: when the transaction occurrence happened; `java.time.Instant`; required and parseable using the standard instant format.
- `ingestion_time`: when the candidate entered the platform boundary; `java.time.Instant`; required and parseable using the standard instant format.
- `amount`: transaction amount; `scala.math.BigDecimal`; required, parseable as a decimal, and greater than zero for `CARD_PAYMENT` in v1.
- `currency`: currency code-shaped value; `String`; exactly three uppercase ASCII letters. This is a format check, not full ISO-4217 membership validation.
- `country`: country code-shaped value; `String`; exactly two uppercase ASCII letters. This is a format check, not full ISO country membership validation.
- `device_id`: device reference; `String`; required and non-blank.
- `ip_address`: observed IPv4 address text; `String`; four local dotted-decimal octets from 0 through 255. Validation performs no DNS or network lookup.
- `transaction_type`: typed transaction category; `TransactionType`; v1 supports only `CARD_PAYMENT`.
- `schema_version`: logical contract version; `Int`; must equal `1`.

## Identity semantics

`event_id` and `transaction_id` are deliberately distinct. `event_id` identifies the particular business-event occurrence. `transaction_id` identifies the business transaction that occurrence describes. They are not interchangeable, and this step defines no duplicate-suppression policy. Kafka topic, partition, and offset metadata are not business identity and are not present in the domain event.

## Time semantics

`event_time` describes when the transaction occurred. `ingestion_time` describes when its candidate entered the platform boundary. The simulator normally generates ingestion time at or after event time, but validation does not impose that relationship as a universal invariant. Watermarks, allowed lateness, and late-event policy remain deferred.

## Validation boundary

```text
untrusted TransactionEventCandidate
        ↓
pure TransactionEventValidator
        ↓
trusted typed TransactionEvent
```

Candidates retain optional string values so missing and malformed boundary data can be represented without exceptions. Validation collects independent typed errors where practical. Only a candidate with no errors becomes a `TransactionEvent`.

## Explicitly deferred decisions

This contract does not select or implement:

- JSON, Avro, or Protobuf;
- Schema Registry;
- Kafka topics or partition keys;
- Kafka metadata representation;
- watermark or late-event policy;
- deduplication policy;
- dead-letter queue behavior;
- schema compatibility mechanisms.
