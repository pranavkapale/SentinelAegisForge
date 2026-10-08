package io.sentinelaegisforge.streaming.simulation

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Path, StandardOpenOption}
import java.security.MessageDigest
import java.time.Instant

import scala.util.Random

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventCandidate,
  TransactionEventValidator
}

final case class SyntheticScenarioRow(
    event: TransactionEvent,
    wave: Int,
    latentProfile: String,
    outcome: String,
    initialOutcome: String,
    firstObservedAt: Instant,
    correctionObservedAt: Option[Instant]
)

final case class SyntheticScenarioCorpus(
    path: Path,
    manifest: Map[String, String],
    rows: Vector[SyntheticScenarioRow]
) {
  def corpusId: String = manifest("corpus_id")
}

/** One inspectable transaction plan and a separate private truth/label artifact.
  *
  * The line formats are fixed-width TSV with ASCII-only generated values. Plan rows contain exactly
  * the existing candidate fields; no label, latent profile, or risk decision is added.
  */
object SyntheticScenarioCorpus {
  private val PlanHeader =
    "event_id\ttransaction_id\tcustomer_id\tmerchant_id\tevent_time\tingestion_time\tamount\tcurrency\tcountry\tdevice_id\tip_address\ttransaction_type\tschema_version\twave"
  private val TruthHeader =
    "event_id\tlatent_profile\toutcome\tinitial_outcome\tfirst_observed_at\tcorrection_observed_at\tseed\tscenario_contract_version"
  private val LabelSource = "SYNTHETIC_SCENARIO_V1"
  private val PlanFile = "plan.tsv"
  private val TruthFile = "truth.tsv"
  private val LabelsFile = "labels.jsonl"
  private val ManifestFile = "manifest.tsv"

  def generate(config: SyntheticScenarioConfig, outputRoot: Path): SyntheticScenarioCorpus = {
    val candidates = new DeterministicTransactionGenerator(
      TransactionGeneratorConfig(config.seed, config.baseTime)
    ).generateValidCandidates(config.transactionCount)
    val random = new Random(config.seed ^ 0x6a09e667f3bcc909L)
    val slotMicros =
      Math.multiplyExact(config.timeHorizonDays.toLong, 86400000000L) / config.transactionCount
    val earlyCustomers = config.customerCount - math.max(1, config.customerCount / 10)
    val rows = candidates.zipWithIndex.map { case (base, index) =>
      val groupPosition = index % 20
      val plannedMicros =
        if (groupPosition < 5)
          (index - groupPosition).toLong * slotMicros + groupPosition.toLong * 5000000L
        else index.toLong * slotMicros
      val ingestion = config.baseTime.plusNanos(plannedMicros * 1000L + 2000000000L)
      val eventTime =
        if (groupPosition == 4) ingestion.minusSeconds(12L)
        else ingestion.minusSeconds(2L)
      val burst = groupPosition < 5
      val large = !burst && random.nextInt(12) == 0
      val campaign = random.nextInt(12) == 0
      val profile =
        (if (burst) "BURST" else if (large) "LARGE" else "REGULAR") +
          (if (campaign) "_CAMPAIGN" else "")
      val customerNumber =
        if (index >= config.transactionCount * 2 / 3 && index % 13 == 0)
          earlyCustomers + random.nextInt(config.customerCount - earlyCustomers)
        else if (burst) (index / 20) % earlyCustomers
        else random.nextInt(earlyCustomers)
      val cents =
        if (large) 500000 + random.nextInt(1000000)
        else if (burst) 1000 + random.nextInt(24000)
        else 500 + random.nextInt(14500)
      val amount = (BigDecimal(cents) / BigDecimal(100)).setScale(4).bigDecimal.toPlainString
      val currency =
        if (burst) { if (groupPosition % 2 == 0) "USD" else "EUR" }
        else if (random.nextInt(100) < config.usdWeightPercent) "USD"
        else "EUR"
      val candidate = base.copy(
        customerId = Some(f"customer-$customerNumber%04d"),
        eventTime = Some(eventTime.toString),
        ingestionTime = Some(ingestion.toString),
        amount = Some(amount),
        currency = Some(currency)
      )
      val event = validated(candidate, index)
      val fraudChancePermille =
        55 + (if (burst) 120 else 0) + (if (large) 95 else 0) +
          (if (campaign) 160 else 0)
      val outcome = if (random.nextInt(1000) < fraudChancePermille) "FRAUD" else "LEGIT"
      val corrected = random.nextInt(100) < 3
      val initialOutcome =
        if (corrected) {
          if (outcome == "FRAUD") "LEGIT" else "FRAUD"
        } else outcome
      val delayHours =
        if (random.nextInt(100) < 8) 720L
        else if (random.nextInt(100) < 25) 72L
        else 6L
      val firstObserved = ingestion.plusSeconds(delayHours * 3600L)
      SyntheticScenarioRow(
        event,
        wave = index * SyntheticScenarioConfig.WaveCount / config.transactionCount,
        profile,
        outcome,
        initialOutcome,
        firstObserved,
        Option.when(corrected)(firstObserved.plusSeconds(96L * 3600L))
      )
    }
    require(rows.map(_.event.eventId).distinct.size == rows.size, "Generated event IDs repeat")
    require(rows.map(_.event.ingestionTime) == rows.map(_.event.ingestionTime).sorted)

    val plan = (PlanHeader +: rows.map(planLine)).mkString("", "\n", "\n").getBytes(UTF_8)
    val truth = (TruthHeader +: rows.map(truthLine(_, config.seed)))
      .mkString("", "\n", "\n")
      .getBytes(UTF_8)
    val labels = rows.flatMap(labelLines).mkString("", "\n", "\n").getBytes(UTF_8)
    val configFields = Map(
      "scenario_contract_version" -> SyntheticScenarioConfig.ContractVersion,
      "seed" -> config.seed.toString,
      "base_time" -> config.baseTime.toString,
      "transaction_count" -> config.transactionCount.toString,
      "customer_count" -> config.customerCount.toString,
      "time_horizon_days" -> config.timeHorizonDays.toString,
      "usd_weight_percent" -> config.usdWeightPercent.toString,
      "behavior_profile" -> config.behaviorProfile,
      "label_delay_profile" -> config.labelDelayProfile
    )
    val configHash = sha256(canonical(configFields))
    val planHash = sha256(plan)
    val truthHash = sha256(truth)
    val corpusId = sha256(
      canonical(
        Map(
          "scenario_contract_version" -> SyntheticScenarioConfig.ContractVersion,
          "generator_config_sha256" -> configHash,
          "plan_sha256" -> planHash,
          "truth_sha256" -> truthHash
        )
      )
    )
    val asOf = config.baseTime.plusSeconds((config.timeHorizonDays + 10L) * 86400L)
    val eligible = rows.count(_.firstObservedAt.compareTo(asOf) <= 0)
    val manifest = configFields ++ Map(
      "generator_config_sha256" -> configHash,
      "corpus_id" -> corpusId,
      "plan_sha256" -> planHash,
      "truth_sha256" -> truthHash,
      "labels_sha256" -> sha256(labels),
      "generated_transaction_count" -> rows.size.toString,
      "unique_event_count" -> rows.map(_.event.eventId).distinct.size.toString,
      "actual_customer_count" -> rows.map(_.event.customerId).distinct.size.toString,
      "usd_count" -> rows.count(_.event.currency == "USD").toString,
      "eur_count" -> rows.count(_.event.currency == "EUR").toString,
      "fraud_count" -> rows.count(_.outcome == "FRAUD").toString,
      "legit_count" -> rows.count(_.outcome == "LEGIT").toString,
      "first_label_available_by_as_of" -> eligible.toString,
      "unlabeled_by_as_of" -> (rows.size - eligible).toString,
      "label_revision_count" -> rows.count(_.correctionObservedAt.nonEmpty).toString,
      "label_record_count" -> rows.map(row => 1 + row.correctionObservedAt.size).sum.toString,
      "label_as_of_time" -> asOf.toString,
      "min_event_time" -> rows.map(_.event.eventTime).min.toString,
      "max_event_time" -> rows.map(_.event.eventTime).max.toString,
      "min_ingestion_time" -> rows.head.event.ingestionTime.toString,
      "max_ingestion_time" -> rows.last.event.ingestionTime.toString,
      "wave_count" -> SyntheticScenarioConfig.WaveCount.toString,
      "scala_version" -> scala.util.Properties.versionNumberString,
      "java_version" -> System.getProperty("java.version")
    ) ++ (0 until SyntheticScenarioConfig.WaveCount).flatMap { wave =>
      val part = rows.filter(_.wave == wave)
      Map(
        s"wave_${wave}_count" -> part.size.toString,
        s"wave_${wave}_min_event_time" -> part.map(_.event.eventTime).min.toString,
        s"wave_${wave}_max_event_time" -> part.map(_.event.eventTime).max.toString
      )
    }
    val root = outputRoot.toAbsolutePath.normalize()
    Files.createDirectories(root)
    val target = root.resolve(corpusId)
    try Files.createDirectory(target)
    catch {
      case _: FileAlreadyExistsException =>
        val existing = inspect(target)
        require(existing.manifest == manifest, s"Existing corpus manifest differs: $target")
        return existing
    }
    writeNew(target.resolve(PlanFile), plan)
    writeNew(target.resolve(TruthFile), truth)
    writeNew(target.resolve(LabelsFile), labels)
    writeNew(target.resolve(ManifestFile), canonical(manifest))
    inspect(target)
  }

  def inspect(path: Path): SyntheticScenarioCorpus = {
    require(Files.isDirectory(path) && !Files.isSymbolicLink(path), s"Not a corpus: $path")
    val manifest = parseManifest(Files.readAllLines(path.resolve(ManifestFile), UTF_8))
    require(manifest("scenario_contract_version") == SyntheticScenarioConfig.ContractVersion)
    require(manifest("corpus_id") == path.getFileName.toString)
    val configKeys = Vector(
      "scenario_contract_version",
      "seed",
      "base_time",
      "transaction_count",
      "customer_count",
      "time_horizon_days",
      "usd_weight_percent",
      "behavior_profile",
      "label_delay_profile"
    )
    require(
      sha256(canonical(configKeys.map(key => key -> manifest(key)).toMap)) ==
        manifest("generator_config_sha256"),
      "Generator configuration hash differs"
    )
    val plan = Files.readAllBytes(path.resolve(PlanFile))
    val truth = Files.readAllBytes(path.resolve(TruthFile))
    val labels = Files.readAllBytes(path.resolve(LabelsFile))
    require(sha256(plan) == manifest("plan_sha256"), "Transaction plan hash differs")
    require(sha256(truth) == manifest("truth_sha256"), "Private truth hash differs")
    require(sha256(labels) == manifest("labels_sha256"), "Synthetic labels hash differs")
    val identity = Map(
      "scenario_contract_version" -> manifest("scenario_contract_version"),
      "generator_config_sha256" -> manifest("generator_config_sha256"),
      "plan_sha256" -> manifest("plan_sha256"),
      "truth_sha256" -> manifest("truth_sha256")
    )
    require(sha256(canonical(identity)) == manifest("corpus_id"), "Corpus identity differs")
    val planLines = new String(plan, UTF_8).linesIterator.toVector
    val truthLines = new String(truth, UTF_8).linesIterator.toVector
    require(planLines.headOption.contains(PlanHeader), "Transaction plan header differs")
    require(truthLines.headOption.contains(TruthHeader), "Private truth header differs")
    require(planLines.size == truthLines.size)
    val seed = manifest("seed").toLong
    val rows =
      planLines.tail.zip(truthLines.tail).zipWithIndex.map { case ((planLine, truthLine), index) =>
        val fields = planLine.split("\t", -1).toVector
        val privateFields = truthLine.split("\t", -1).toVector
        require(fields.size == 14 && privateFields.size == 8)
        require(fields.head == privateFields.head, s"Truth event ID differs at index $index")
        require(privateFields(6) == seed.toString)
        require(privateFields(7) == SyntheticScenarioConfig.ContractVersion)
        val candidate = TransactionEventCandidate(
          Some(fields(0)),
          Some(fields(1)),
          Some(fields(2)),
          Some(fields(3)),
          Some(fields(4)),
          Some(fields(5)),
          Some(fields(6)),
          Some(fields(7)),
          Some(fields(8)),
          Some(fields(9)),
          Some(fields(10)),
          Some(fields(11)),
          Some(fields(12))
        )
        val event = validated(candidate, index)
        val first = Instant.parse(privateFields(4))
        val correction = Option.when(privateFields(5).nonEmpty)(
          Instant.parse(privateFields(5))
        )
        require(first.compareTo(event.ingestionTime) >= 0)
        require(correction.forall(_.compareTo(first) >= 0))
        require(Set("FRAUD", "LEGIT").contains(privateFields(2)))
        require(Set("FRAUD", "LEGIT").contains(privateFields(3)))
        SyntheticScenarioRow(
          event,
          fields(13).toInt,
          privateFields(1),
          privateFields(2),
          privateFields(3),
          first,
          correction
        )
      }
    require(rows.size == manifest("generated_transaction_count").toInt)
    require(rows.map(_.event.eventId).distinct.size == rows.size)
    require(rows.map(_.event.ingestionTime) == rows.map(_.event.ingestionTime).sorted)
    require(rows.forall(row => row.wave >= 0 && row.wave < SyntheticScenarioConfig.WaveCount))
    require(
      rows.zipWithIndex.forall { case (row, index) =>
        row.wave == index * SyntheticScenarioConfig.WaveCount / rows.size
      }
    )
    SyntheticScenarioCorpus(path, manifest, rows)
  }

  private def planLine(row: SyntheticScenarioRow): String = {
    val event = row.event
    Vector(
      event.eventId.toString,
      event.transactionId,
      event.customerId,
      event.merchantId,
      event.eventTime.toString,
      event.ingestionTime.toString,
      event.amount.bigDecimal.toPlainString,
      event.currency,
      event.country,
      event.deviceId,
      event.ipAddress,
      event.transactionType.externalName,
      event.schemaVersion.toString,
      row.wave.toString
    ).mkString("\t")
  }

  private def truthLine(row: SyntheticScenarioRow, seed: Long): String =
    Vector(
      row.event.eventId.toString,
      row.latentProfile,
      row.outcome,
      row.initialOutcome,
      row.firstObservedAt.toString,
      row.correctionObservedAt.fold("")(_.toString),
      seed.toString,
      SyntheticScenarioConfig.ContractVersion
    ).mkString("\t")

  private def labelLines(row: SyntheticScenarioRow): Vector[String] = {
    def line(outcome: String, observed: Instant, revision: Int): String =
      s"""{"event_id":"${row.event.eventId}","outcome":"$outcome","label_observed_at":"$observed","label_source":"$LabelSource","label_revision":$revision}"""
    Vector(line(row.initialOutcome, row.firstObservedAt, 1)) ++
      row.correctionObservedAt.toVector.map(line(row.outcome, _, 2))
  }

  private def validated(candidate: TransactionEventCandidate, index: Int): TransactionEvent =
    TransactionEventValidator
      .validate(candidate)
      .fold(
        errors =>
          throw new IllegalArgumentException(
            s"Scenario candidate $index invalid: ${errors.mkString(", ")}"
          ),
        identity
      )

  private def canonical(fields: Map[String, String]): Array[Byte] =
    fields.toVector
      .sortBy(_._1)
      .map { case (key, value) =>
        require(!key.contains("\t") && !value.contains("\t") && !value.contains("\n"))
        s"$key\t$value\n"
      }
      .mkString
      .getBytes(UTF_8)

  private def parseManifest(lines: java.util.List[String]): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    val pairs = lines.asScala.toVector.map { line =>
      val parts = line.split("\t", 2)
      require(parts.size == 2, "Malformed scenario manifest")
      parts(0) -> parts(1)
    }
    require(pairs.map(_._1).distinct.size == pairs.size, "Duplicate manifest key")
    pairs.toMap
  }

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(byte => f"${byte & 0xff}%02x").mkString

  private def writeNew(path: Path, bytes: Array[Byte]): Unit =
    Files.write(path, bytes, StandardOpenOption.CREATE_NEW)
}
