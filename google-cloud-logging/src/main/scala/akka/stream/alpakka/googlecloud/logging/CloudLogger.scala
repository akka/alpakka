/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging

import akka.Done
import akka.actor.{Actor, ActorRef, ExtendedActorSystem, Status}
import akka.dispatch.RequiresMessageQueue
import akka.event.Logging._
import akka.event.{LoggerMessageQueueSemantics, Logging, LoggingBus}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.pattern.pipe
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.googlecloud.logging.impl.{Resource, WithInsertId}
import akka.stream.alpakka.googlecloud.logging.model.{LogEntry, LogSeverity, MonitoredResource, WriteEntriesRequest}
import akka.stream.alpakka.googlecloud.logging.scaladsl.CloudLogging
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.{ActorAttributes, CompletionStrategy, Materializer, OverflowStrategy}
import akka.util.Helpers
import akka.util.JavaDurationConverters._
import spray.json.{DefaultJsonProtocol, JsonFormat}

import java.io.{PrintWriter, StringWriter}
import java.time.Instant
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * Cloud Logging logger.
 *
 * The thread in which the logging was performed is captured in
 * the log entry labels with attribute name "sourceThread".
 */
final class CloudLogger extends Actor with RequiresMessageQueue[LoggerMessageQueueSemantics] {
  // Heavily based on akka.event.slf4j.Slf4jLogger and com.google.cloud.logging.logback.LoggingAppender

  private val log = Logging(context.system, this)

  private var sink: ActorRef = context.system.deadLetters
  private var bus: LoggingBus = _

  private val mdcThreadAttributeName = "sourceThread"
  private val mdcActorSystemAttributeName = "sourceActorSystem"
  private val mdcAkkaSourceAttributeName = "akkaSource"
  private val mdcAkkaTimestamp = "akkaTimestamp"
  private val mdcAkkaAddressAttributeName = "akkaAddress"

  private def akkaAddress = context.system.asInstanceOf[ExtendedActorSystem].provider.addressString

  override def receive: Receive = {

    case event: LogEvent =>
      sink ! event

    case InitializeLogger(bus) =>
      import context.dispatcher

      this.bus = bus
      log.info("CloudLogger starting...")

      val sink = for {
        _ <- Future.unit // Initializes a Future to capture any subsequent exceptions
        config = context.system.settings.config.getConfig("alpakka.google.logger")
        googleSettings = GoogleSettings(config.getString("google-settings"))(context.system)
        resource <- getResource(googleSettings.projectId, config.getString("resource-type"))
        template = WriteEntriesRequest()
          .withLogName(s"projects/${googleSettings.projectId}/logs/${config.getString("log")}")
          .withResource(resource)
        settings = WriteEntriesSettings(
          config.getInt("queue-size"),
          Try(config.getInt("flush-severity"))
            .map(LogSeverity(_))
            .getOrElse(LogSeverity(config.getString("flush-severity"))),
          config.getDuration("flush-within").asScala,
          parseOverflowStrategy(config.getString("overflow-strategy")),
          template
        )
      } yield {

        implicit val mat = Materializer(context.system)

        Source
          .actorRef[LogEvent](
            {
              case Done => CompletionStrategy.draining
            }: PartialFunction[Any, CompletionStrategy],
            PartialFunction.empty,
            1,
            OverflowStrategy.fail
          )
          .map(logEntryFor)
          .via(WithInsertId[Payload])
          .toMat(CloudLogging.writeEntries(settings))(Keep.both)
          .addAttributes(ActorAttributes.dispatcher(context.system.settings.LoggersDispatcher))
          .run()
      }

      val forwardTo = sender
      sink.transform(result => Success(MaterializedSink(result, forwardTo))).pipeTo(self)

    case MaterializedSink(Success((ref, complete)), forwardTo) =>
      import context.dispatcher
      sink = ref
      forwardTo ! LoggerInitialized
      log.info("CloudLogger started")
      complete.transform(result => Success(SinkCompleted(result))).pipeTo(self).foreach {
        case SinkCompleted(Success(Done)) =>
          log.info("CloudLogger stopped")
        case SinkCompleted(Failure(ex)) =>
          log.error(ex, "CloudLogger errored")
      }

    case MaterializedSink(Failure(ex), forwardTo) =>
      forwardTo ! Status.Failure(ex)
      log.error(ex, "CloudLogger failed to start")
      throw ex

    case SinkCompleted(Success(Done)) =>
    // Do nothing
    case SinkCompleted(Failure(ex)) =>
      throw ex
  }

  override def postStop(): Unit = {
    log.info("CloudLogger stopping...")
    if (bus != null) bus.unsubscribe(context.self)
    sink ! Done
  }

  private def getResource(projectId: String, resourceType: String) = {
    implicit val mat = Materializer(context)
    import mat.executionContext
    for {
      resource <- if (resourceType.isEmpty)
        Resource.autoDetect
      else
        Future.successful(Resource(resourceType))
      labels <- resource.labels
    } yield MonitoredResource(resource.`type`, labels + ("project_id" -> projectId))
  }

  private def logEntryFor(event: LogEvent): LogEntry[Payload] = {

    val severity = severityFor(event.level)

    val message = Option(event.message).map(_.toString).orElse {
      event match {
        case e: LogEventWithCause => Some(e.cause.getLocalizedMessage)
        case _ => None
      }
    }

    val cause = event match {
      case e: LogEventWithCause =>
        val writer = new PrintWriter(new StringWriter)
        e.cause.printStackTrace(writer)
        Some(writer.toString)
      case _ => None
    }

    val `@type` = severity match {
      case LogSeverity.Error =>
        Some("type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent")
      case _ => None
    }

    val payload = Payload(message, cause, `@type`)

    val markerLabels = event match {
      case e: LogEventWithMarker =>
        e.marker.properties.mapValues(String.valueOf)
      case _ => Map.empty
    }

    val labels = Map(
        "akkaLogLevel" -> event.level.asInt.toString,
        mdcAkkaSourceAttributeName -> event.logSource,
        mdcThreadAttributeName -> event.thread.getName,
        mdcAkkaTimestamp -> formatTimestamp(event.timestamp),
        mdcActorSystemAttributeName -> context.system.name,
        mdcAkkaAddressAttributeName -> akkaAddress
      ) ++ event.mdc.mapValues(String.valueOf) ++ markerLabels

    LogEntry(payload)
      .withTimestamp(Instant.ofEpochMilli(event.timestamp))
      .withSeverity(severity)
      .withLabels(labels)
  }

  private def severityFor(level: LogLevel): LogSeverity = level match {
    case level if level <= LogLevel(-2) => LogSeverity.Emergency
    case LogLevel(-1) => LogSeverity.Alert
    case LogLevel(0) => LogSeverity.Critical
    case ErrorLevel => LogSeverity.Error
    case WarningLevel => LogSeverity.Warning
    // LogSeverity.Notice would go here
    case InfoLevel => LogSeverity.Info
    case level if level >= DebugLevel => LogSeverity.Debug
  }

  private def parseOverflowStrategy(strategy: String) = Helpers.toRootLowerCase(strategy) match {
    case "drop-head" => OverflowStrategy.dropHead
    case "drop-tail" => OverflowStrategy.dropTail
    case x => throw new IllegalArgumentException(s"Unrecognized or unsupported overflow strategy: ${x}")
  }

  /**
   * Override this method to provide a differently formatted timestamp
   * @param timestamp a "currentTimeMillis"-obtained timestamp
   * @return the given timestamp as a UTC String
   */
  protected def formatTimestamp(timestamp: Long): String =
    Helpers.currentTimeMillisToUTCString(timestamp)
}

private final case class MaterializedSink(result: Try[(ActorRef, Future[Done])], forwardTo: ActorRef)

private final case class SinkCompleted(result: Try[Done])

private final case class Payload(message: Option[String], cause: Option[String], `@type`: Option[String])
private object Payload extends DefaultJsonProtocol {
  implicit val format: JsonFormat[Payload] = jsonFormat3(apply)
}
