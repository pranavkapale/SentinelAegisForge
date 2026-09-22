package io.sentinelaegisforge.streaming.kafka.producer

import java.time.Duration
import java.util.concurrent.{ExecutionException, Future}

import scala.util.control.NonFatal

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{Producer, RecordMetadata}

import io.sentinelaegisforge.streaming.domain.transaction.TransactionEvent
import io.sentinelaegisforge.streaming.kafka.producer.TransactionPublishError._

final class TransactionEventPublisher(
    producer: Producer[String, GenericRecord],
    recordFactory: TransactionProducerRecordFactory,
    closeTimeout: Duration = Duration.ofSeconds(10)
) {
  require(!closeTimeout.isNegative, "closeTimeout must not be negative")

  def publish(
      events: Vector[TransactionEvent]
  ): Either[TransactionPublishError, TransactionPublishReport] = {
    val publishResult =
      try publishOpenProducer(events)
      catch {
        case NonFatal(error) => Left(ProducerOperationFailed(errorMessage(error)))
      }

    val closeResult =
      try {
        producer.close(closeTimeout)
        None
      } catch {
        case NonFatal(error) => Some(ProducerCloseFailed(errorMessage(error)))
      }

    closeResult.map(Left(_)).getOrElse(publishResult)
  }

  private def publishOpenProducer(
      events: Vector[TransactionEvent]
  ): Either[TransactionPublishError, TransactionPublishReport] = {
    val records = Vector.newBuilder[
      (
          Int,
          TransactionEvent,
          org.apache.kafka.clients.producer.ProducerRecord[String, GenericRecord]
      )
    ]

    events.zipWithIndex.foreach { case (event, index) =>
      recordFactory.create(event) match {
        case Right(record) => records += ((index, event, record))
        case Left(error)   => return Left(RecordConstructionFailed(index, event.eventId, error))
      }
    }

    val pending = records.result().map { case (index, event, record) =>
      val future =
        try Right(producer.send(record))
        catch {
          case NonFatal(error) => Left(error)
        }
      (index, event, future)
    }

    producer.flush()

    val acknowledgements = Vector.newBuilder[PublishedTransactionMetadata]
    val failures = Vector.newBuilder[TransactionPublishFailure]

    pending.foreach { case (index, event, result) =>
      result match {
        case Right(future) =>
          awaitMetadata(future) match {
            case Right(metadata) =>
              acknowledgements += PublishedTransactionMetadata(
                topic = metadata.topic(),
                partition = metadata.partition(),
                offset = metadata.offset()
              )
            case Left(details) =>
              failures += TransactionPublishFailure(index, event.eventId, details)
          }
        case Left(error) =>
          failures += TransactionPublishFailure(index, event.eventId, errorMessage(error))
      }
    }

    val report = TransactionPublishReport(
      requested = events.size,
      acknowledged = acknowledgements.result(),
      failures = failures.result()
    )

    if (report.failures.isEmpty) Right(report) else Left(DeliveryFailed(report))
  }

  private def awaitMetadata(future: Future[RecordMetadata]): Either[String, RecordMetadata] =
    try Right(future.get())
    catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        Left(errorMessage(interrupted))
      case execution: ExecutionException =>
        Left(errorMessage(Option(execution.getCause).getOrElse(execution)))
      case NonFatal(error) => Left(errorMessage(error))
    }

  private def errorMessage(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
}
