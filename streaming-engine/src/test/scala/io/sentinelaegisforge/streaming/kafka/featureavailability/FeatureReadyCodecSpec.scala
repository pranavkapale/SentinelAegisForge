package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite

final class FeatureReadyCodecSpec extends AnyFunSuite {
  private val golden =
    Files.readAllBytes(Paths.get("..", "contracts", "test-fixtures", "feature-ready-v1.jsonl"))

  test("Python and Scala agree on canonical ready-event bytes, identity and Kafka key") {
    val event = FeatureReadyCodec.decode(golden)
    assert(
      event.readyEventId ==
        "2c3f349ae1c0a051f51c81c9ab557ea480265aac5ee5532f1cdaa96b2b61d369"
    )
    assert(FeatureReadyCodec.validateRecord(FeatureReadyCodec.key(event), golden) == event)
    assert(
      java.util.Arrays
        .equals(FeatureReadyCodec.canonical(FeatureReadyCodec.parseJson(golden)), golden)
    )
  }

  test("canonical JSON preserves Unicode and large integral values") {
    val bytes = FeatureReadyCodec.objectBytes("z" -> "café", "a" -> Long.MaxValue)
    assert(
      new String(bytes, StandardCharsets.UTF_8) ==
        "{\"a\":9223372036854775807,\"z\":\"café\"}\n"
    )
  }

  test("unknown fields, duplicate keys, unsupported versions and states fail") {
    val text = new String(golden, StandardCharsets.UTF_8)
    val unknown = text.replace("\"availability_state\":", "\"other\":1,\"availability_state\":")
    intercept[IllegalArgumentException](
      FeatureReadyCodec.decode(unknown.getBytes(StandardCharsets.UTF_8))
    )
    val repeated = text.replace(
      "\"availability_state\":",
      "\"availability_state\":\"DELTA_COMMITTED\",\"availability_state\":"
    )
    intercept[Exception](FeatureReadyCodec.decode(repeated.getBytes(StandardCharsets.UTF_8)))
    intercept[IllegalArgumentException](
      FeatureReadyCodec.decode(
        text
          .replace("feature-availability-v1", "feature-availability-v9")
          .getBytes(StandardCharsets.UTF_8)
      )
    )
    intercept[IllegalArgumentException](
      FeatureReadyCodec.decode(
        text.replace("DELTA_COMMITTED", "UNCOMMITTED").getBytes(StandardCharsets.UTF_8)
      )
    )
  }

  test("invalid fingerprints, table identity and Kafka key fail") {
    val text = new String(golden, StandardCharsets.UTF_8)
    intercept[IllegalArgumentException](
      FeatureReadyCodec.decode(
        text.replace("f79cac8a", "xxxxxxxx").getBytes(StandardCharsets.UTF_8)
      )
    )
    intercept[IllegalArgumentException](
      FeatureReadyCodec.decode(
        text
          .replace("63b8d48b-ea0b-47e2-aec9-ad5c6990d5de", "invalid-table")
          .getBytes(StandardCharsets.UTF_8)
      )
    )
    intercept[IllegalArgumentException](
      FeatureReadyCodec.validateRecord("wrong-key".getBytes(StandardCharsets.UTF_8), golden)
    )
    intercept[IllegalArgumentException](FeatureReadyCodec.validateRecord(null, golden))
  }
}
