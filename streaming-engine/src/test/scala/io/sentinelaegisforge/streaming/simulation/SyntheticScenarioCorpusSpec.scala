package io.sentinelaegisforge.streaming.simulation

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite

final class SyntheticScenarioCorpusSpec extends AnyFunSuite {
  private val config = SyntheticScenarioConfig(
    seed = 714L,
    baseTime = Instant.parse("2030-01-01T00:00:00Z"),
    transactionCount = 1500,
    customerCount = 90
  )

  test("same configuration reproduces plan, private truth, labels and corpus identity") {
    val first = SyntheticScenarioCorpus.generate(config, Files.createTempDirectory("scenario-a"))
    val second = SyntheticScenarioCorpus.generate(config, Files.createTempDirectory("scenario-b"))
    assert(first.corpusId == second.corpusId)
    assert(first.manifest == second.manifest)
    Vector("plan.tsv", "truth.tsv", "labels.jsonl", "manifest.tsv").foreach { name =>
      assert(
        Files.readAllBytes(first.path.resolve(name)).toVector ==
          Files.readAllBytes(second.path.resolve(name)).toVector
      )
    }
    assert(
      SyntheticScenarioCorpus.generate(config, first.path.getParent).corpusId == first.corpusId
    )
    assert(first.rows.map(_.event.eventId) == second.rows.map(_.event.eventId))
    assert(first.rows.map(_.outcome) == second.rows.map(_.outcome))
    assert(
      SyntheticScenarioCorpus
        .generate(config.copy(seed = 715L), Files.createTempDirectory("scenario-c"))
        .corpusId != first.corpusId
    )
  }

  test(
    "population has recurrent and later-new customers, mixed currencies and overlapping outcomes"
  ) {
    val corpus = SyntheticScenarioCorpus.generate(config, Files.createTempDirectory("scenario-d"))
    assert(corpus.rows.size == 1500)
    assert(corpus.rows.map(_.event.eventId).distinct.size == 1500)
    assert(
      corpus.rows.map(_.wave).groupBy(identity).view.mapValues(_.size).toMap == Map(
        0 -> 500,
        1 -> 500,
        2 -> 500
      )
    )
    assert(corpus.rows.forall(row => row.event.ingestionTime.compareTo(row.event.eventTime) >= 0))
    assert(corpus.rows.sliding(2).exists {
      case Vector(earlier, later) => later.event.eventTime.isBefore(earlier.event.eventTime)
      case _                      => false
    })
    assert(corpus.rows.map(_.event.currency).toSet == Set("USD", "EUR"))
    val byCustomer = corpus.rows.groupBy(_.event.customerId)
    assert(byCustomer.values.exists(rows => rows.map(_.event.currency).toSet.size == 2))
    assert(byCustomer.values.exists(_.size > 5))
    val earlyCustomers = corpus.rows.filter(_.wave == 0).map(_.event.customerId).toSet
    val laterCustomers = corpus.rows.filter(_.wave == 2).map(_.event.customerId).toSet
    assert(earlyCustomers.intersect(laterCustomers).nonEmpty)
    assert(laterCustomers.diff(earlyCustomers).nonEmpty)
    val byProfile = corpus.rows.groupBy(_.latentProfile)
    assert(byProfile("BURST").map(_.outcome).toSet == Set("FRAUD", "LEGIT"))
    assert(byProfile("REGULAR").map(_.outcome).toSet == Set("FRAUD", "LEGIT"))
    assert(corpus.manifest("unlabeled_by_as_of").toInt > 0)
    assert(corpus.manifest("label_revision_count").toInt > 0)
    val plan = Files.readString(corpus.path.resolve("plan.tsv"), UTF_8)
    assert(!plan.contains("outcome") && !plan.contains("latent_profile"))
    assert(!plan.contains("risk_disposition") && !plan.contains("fraud_label"))
  }

  test("corpus inspection rejects changed plan, duplicated event identity and invalid config") {
    val corpus = SyntheticScenarioCorpus.generate(config, Files.createTempDirectory("scenario-e"))
    val plan = corpus.path.resolve("plan.tsv")
    val original = Files.readString(plan, UTF_8)
    Files.writeString(plan, original.replaceFirst("customer-0000", "customer-9999"), UTF_8)
    intercept[IllegalArgumentException] {
      SyntheticScenarioCorpus.inspect(corpus.path)
    }
    intercept[IllegalArgumentException] {
      config.copy(transactionCount = 2001)
    }
    intercept[IllegalArgumentException] {
      config.copy(usdWeightPercent = 100)
    }
  }

  test("a marked publication wave is refused before opening a Kafka producer") {
    val corpus = SyntheticScenarioCorpus.generate(config, Files.createTempDirectory("scenario-f"))
    Files.writeString(corpus.path.resolve("publish-attempt-wave-0.tsv"), "attempted\n", UTF_8)
    val error = intercept[IllegalStateException] {
      SyntheticScenarioApp.main(
        Array(
          "publish",
          s"--corpus=${corpus.path}",
          "--wave=0",
          "--bootstrap-servers=localhost:9092",
          "--schema-registry-url=http://localhost:8081"
        )
      )
    }
    assert(error.getMessage.contains("implicit replay is refused"))
    assert(!Files.exists(corpus.path.resolve("publish-report-wave-0.tsv")))
  }
}
