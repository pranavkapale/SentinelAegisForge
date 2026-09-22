package io.sentinelaegisforge.streaming.kafka.producer

import java.time.Instant

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventValidator
}
import io.sentinelaegisforge.streaming.kafka.producer.TransactionPublishError.DeliveryFailed
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroSchema
import io.sentinelaegisforge.streaming.simulation.{
  DeterministicTransactionGenerator,
  TransactionGeneratorConfig
}

object TransactionProducerApp {
  private val DefaultCount = 10
  private val DefaultSeed = 42L
  private val DefaultBaseTime = Instant.parse("2026-09-21T00:00:00Z")
  private val DefaultBootstrapServers = "localhost:9092"
  private val DefaultSchemaRegistryUrl = "http://localhost:8081"

  def main(args: Array[String]): Unit = {
    val applicationConfig =
      parseArguments(args).fold(message => throw new IllegalArgumentException(message), identity)
    val producerConfig = TransactionProducerConfig(
      bootstrapServers = applicationConfig.bootstrapServers,
      schemaRegistryUrl = applicationConfig.schemaRegistryUrl,
      topic = applicationConfig.topic
    )
    val schema = TransactionEventAvroSchema.load.fold(
      error => throw new IllegalStateException(error.message),
      identity
    )
    val candidates = new DeterministicTransactionGenerator(
      TransactionGeneratorConfig(
        seed = applicationConfig.seed,
        baseEventTime = applicationConfig.baseTime
      )
    ).generateValidCandidates(applicationConfig.count)
    val events = candidates.zipWithIndex.map { case (candidate, index) =>
      TransactionEventValidator
        .validate(candidate)
        .fold(
          errors =>
            throw new IllegalStateException(
              s"Generated candidate $index failed validation: ${errors.mkString(", ")}"
            ),
          identity
        )
    }

    val producer = new KafkaProducer[String, GenericRecord](
      TransactionProducerConfig.properties(producerConfig)
    )
    val publisher = new TransactionEventPublisher(
      producer,
      new TransactionProducerRecordFactory(producerConfig.topic, schema)
    )

    publisher.publish(events) match {
      case Right(report)                => printReport(report)
      case Left(DeliveryFailed(report)) =>
        printReport(report)
        throw new IllegalStateException(
          report.failures.map(_.details).mkString("Transaction publishing failed: ", "; ", "")
        )
      case Left(error) => throw new IllegalStateException(error.message)
    }
  }

  private def printReport(report: TransactionPublishReport): Unit = {
    println(
      s"Transaction publish result: requested=${report.requested} " +
        s"acknowledged=${report.acknowledgedCount} failed=${report.failedCount}"
    )
    report.acknowledged.foreach { metadata =>
      println(
        s"Acknowledged: topic=${metadata.topic} partition=${metadata.partition} " +
          s"offset=${metadata.offset}"
      )
    }
  }

  private def parseArguments(args: Array[String]): Either[String, ApplicationConfig] = {
    val parsed = args.foldLeft[Either[String, Map[String, String]]](Right(Map.empty)) {
      case (Right(values), argument) if argument.startsWith("--") && argument.contains("=") =>
        val parts = argument.drop(2).split("=", 2)
        if (parts(0).nonEmpty && parts(1).nonEmpty) Right(values.updated(parts(0), parts(1)))
        else Left(s"Invalid argument: $argument")
      case (Right(_), argument) => Left(s"Expected --name=value argument, received: $argument")
      case (left @ Left(_), _)  => left
    }

    parsed.flatMap { values =>
      for {
        count <- parseInt(values.getOrElse("count", DefaultCount.toString), "count")
        _ <- Either.cond(count >= 0, (), "count must not be negative")
        seed <- parseLong(values.getOrElse("seed", DefaultSeed.toString), "seed")
        baseTime <- parseInstant(
          values.getOrElse("base-time", DefaultBaseTime.toString),
          "base-time"
        )
        config <- Either.cond(
          values.keySet.subsetOf(SupportedArguments),
          ApplicationConfig(
            count = count,
            seed = seed,
            baseTime = baseTime,
            bootstrapServers = values.getOrElse("bootstrap-servers", DefaultBootstrapServers),
            schemaRegistryUrl = values.getOrElse(
              "schema-registry-url",
              DefaultSchemaRegistryUrl
            ),
            topic = values.getOrElse("topic", TransactionProducerConfig.DefaultTopic)
          ),
          s"Unsupported arguments: ${(values.keySet -- SupportedArguments).toVector.sorted.mkString(", ")}"
        )
      } yield config
    }
  }

  private def parseInt(value: String, name: String): Either[String, Int] =
    scala.util.Try(value.toInt).toEither.left.map(_ => s"$name must be an integer: $value")

  private def parseLong(value: String, name: String): Either[String, Long] =
    scala.util.Try(value.toLong).toEither.left.map(_ => s"$name must be a long integer: $value")

  private def parseInstant(value: String, name: String): Either[String, Instant] =
    scala.util.Try(Instant.parse(value)).toEither.left.map(_ => s"$name must be an instant: $value")

  private val SupportedArguments = Set(
    "count",
    "seed",
    "base-time",
    "bootstrap-servers",
    "schema-registry-url",
    "topic"
  )

  private final case class ApplicationConfig(
      count: Int,
      seed: Long,
      baseTime: Instant,
      bootstrapServers: String,
      schemaRegistryUrl: String,
      topic: String
  )
}
