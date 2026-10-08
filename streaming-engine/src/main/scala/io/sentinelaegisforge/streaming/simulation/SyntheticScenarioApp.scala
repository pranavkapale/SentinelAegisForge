package io.sentinelaegisforge.streaming.simulation

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.Instant

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer

import io.sentinelaegisforge.streaming.kafka.producer.{
  TransactionEventPublisher,
  TransactionProducerConfig,
  TransactionProducerRecordFactory,
  TransactionPublishError,
  TransactionPublishReport
}
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroSchema

/** Generate/inspect before publishing; publish each bounded chronological wave at most once. */
object SyntheticScenarioApp {
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Expected generate, inspect, or publish")
    val values = args.tail.map { argument =>
      require(argument.startsWith("--") && argument.contains("="), s"Invalid argument: $argument")
      val parts = argument.drop(2).split("=", 2)
      require(parts(0).nonEmpty && parts(1).nonEmpty)
      parts(0) -> parts(1)
    }.toVector
    require(values.map(_._1).distinct.size == values.size, "Duplicate argument")
    val options = values.toMap
    args.head match {
      case "generate" =>
        requireKnown(
          options,
          Set(
            "seed",
            "base-time",
            "count",
            "customers",
            "horizon-days",
            "usd-weight-percent",
            "output-root"
          )
        )
        val config = SyntheticScenarioConfig(
          seed = required(options, "seed").toLong,
          baseTime = Instant.parse(required(options, "base-time")),
          transactionCount = options.getOrElse("count", "1500").toInt,
          customerCount = options.getOrElse("customers", "90").toInt,
          timeHorizonDays = options.getOrElse("horizon-days", "9").toInt,
          usdWeightPercent = options.getOrElse("usd-weight-percent", "65").toInt
        )
        report(SyntheticScenarioCorpus.generate(config, Path.of(required(options, "output-root"))))
      case "inspect" =>
        requireKnown(options, Set("corpus"))
        report(SyntheticScenarioCorpus.inspect(Path.of(required(options, "corpus"))))
      case "publish" =>
        requireKnown(options, Set("corpus", "wave", "bootstrap-servers", "schema-registry-url"))
        val corpus = SyntheticScenarioCorpus.inspect(Path.of(required(options, "corpus")))
        val wave = required(options, "wave").toInt
        require(wave >= 0 && wave < SyntheticScenarioConfig.WaveCount, "Invalid wave")
        publish(
          corpus,
          wave,
          TransactionProducerConfig(
            options.getOrElse("bootstrap-servers", "localhost:9092"),
            options.getOrElse("schema-registry-url", "http://localhost:8081")
          )
        )
      case other => throw new IllegalArgumentException(s"Unknown scenario command: $other")
    }
  }

  private def publish(
      corpus: SyntheticScenarioCorpus,
      wave: Int,
      config: TransactionProducerConfig
  ): Unit = {
    val events = corpus.rows.filter(_.wave == wave).map(_.event)
    require(events.nonEmpty, s"Empty wave $wave")
    val schema = TransactionEventAvroSchema.load.fold(
      error => throw new IllegalStateException(error.message),
      identity
    )
    val attempt = corpus.path.resolve(s"publish-attempt-wave-$wave.tsv")
    try {
      Files.write(
        attempt,
        s"corpus_id\t${corpus.corpusId}\nwave\t$wave\nexpected\t${events.size}\n"
          .getBytes(UTF_8),
        StandardOpenOption.CREATE_NEW
      )
    } catch {
      case _: java.nio.file.FileAlreadyExistsException =>
        throw new IllegalStateException(
          s"Wave $wave already has a publish attempt; implicit replay is refused: $attempt"
        )
    }
    val producer = new KafkaProducer[String, GenericRecord](
      TransactionProducerConfig.properties(config)
    )
    val publisher = new TransactionEventPublisher(
      producer,
      new TransactionProducerRecordFactory(config.topic, schema)
    )
    val result = publisher.publish(events)
    val report = result match {
      case Right(value)                                        => value
      case Left(TransactionPublishError.DeliveryFailed(value)) => value
      case Left(error) => throw new IllegalStateException(error.message)
    }
    writePublishReport(corpus.path, wave, report)
    println(
      s"Synthetic wave publish: corpus=${corpus.corpusId} wave=$wave expected=${events.size} acknowledged=${report.acknowledgedCount} failed=${report.failedCount}"
    )
    if (report.acknowledgedCount != events.size || report.failedCount != 0)
      throw new IllegalStateException(s"Synthetic wave $wave did not receive every acknowledgement")
  }

  private def writePublishReport(
      path: Path,
      wave: Int,
      report: TransactionPublishReport
  ): Unit = {
    val header =
      s"requested\t${report.requested}\nacknowledged\t${report.acknowledgedCount}\nfailed\t${report.failedCount}\n"
    val acknowledgements = report.acknowledged.map { value =>
      s"ack\t${value.topic}\t${value.partition}\t${value.offset}\n"
    }.mkString
    val failures = report.failures.map { value =>
      s"failure\t${value.index}\t${value.eventId}\t${value.details}\n"
    }.mkString
    Files.write(
      path.resolve(s"publish-report-wave-$wave.tsv"),
      (header + acknowledgements + failures).getBytes(UTF_8),
      StandardOpenOption.CREATE_NEW
    )
  }

  private def report(corpus: SyntheticScenarioCorpus): Unit = {
    println(s"Synthetic corpus: ${corpus.path}")
    corpus.manifest.toVector.sortBy(_._1).foreach { case (key, value) =>
      println(s"$key=$value")
    }
  }

  private def required(options: Map[String, String], key: String): String =
    options.getOrElse(key, throw new IllegalArgumentException(s"Missing --$key"))

  private def requireKnown(options: Map[String, String], allowed: Set[String]): Unit =
    require(
      options.keySet.subsetOf(allowed),
      s"Unknown arguments: ${(options.keySet -- allowed).toVector.sorted.mkString(", ")}"
    )
}
