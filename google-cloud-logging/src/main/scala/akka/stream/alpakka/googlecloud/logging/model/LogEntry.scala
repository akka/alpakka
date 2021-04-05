/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.model

import akka.annotation.DoNotInherit
import akka.http.scaladsl.model.Uri.Path
import akka.stream.alpakka.googlecloud.logging.model.PathJsonProtocol._
import akka.util.Helpers
import spray.json.DefaultJsonProtocol._
import spray.json.{deserializationError, JsNumber, JsString, JsValue, JsonFormat, JsonWriter}

import java.time.Instant
import java.util
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.util.Try

/**
 * An individual entry in a log.
 * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry Cloud Logging Reference]]
 *
 * @param logName The resource name of the log to which this log entry belongs.
 * @param resource The monitored resource that produced this log entry.
 * @param timestamp The time the event described by the log entry occurred.
 * @param severity The severity of the log entry.
 * @param insertId A unique identifier for the log entry.
 * @param labels A set of user-defined (key, value) data that provides additional information about the log entry.
 * @param jsonPayload The log entry payload, represented as a structure that is expressed as a JSON object.
 * @tparam T The data model for the `jsonPayload`.
 */
final case class LogEntry[+T] private (logName: Option[Path],
                                       resource: Option[MonitoredResource],
                                       timestamp: Option[Instant],
                                       severity: Option[LogSeverity],
                                       insertId: Option[String],
                                       labels: Map[String, String],
                                       jsonPayload: T) {

  def getLogName = logName.map(_.asString).asJava
  def getResource = resource.asJava
  def getTimestamp = timestamp.asJava
  def getSeverity = severity.asJava
  def getInsertId = insertId.asJava
  def getLabels = labels.asJava
  def getJsonPayload = jsonPayload

  def withLogName(logName: Path) =
    copy(logName = Some(logName))
  def withLogName(logName: String) =
    copy(logName = Some(Path(logName)))
  def withoutLogName =
    copy(logName = None)

  def withResource(resource: MonitoredResource) =
    copy(resource = Some(resource))
  def withoutResource =
    copy(resource = None)

  def withTimestamp(timestamp: Instant) =
    copy(timestamp = Some(timestamp))
  def withoutTimestamp =
    copy(timestamp = None)

  def withSeverity(severity: LogSeverity) =
    copy(severity = Some(severity))
  def withoutSeverity =
    copy(severity = None)

  def withInsertId(insertId: String) =
    copy(insertId = Some(insertId))
  def withoutInsertId =
    copy(insertId = None)

  def withLabels(labels: Map[String, String]) =
    copy(labels = labels)
  def withLabels(labels: util.Map[String, String]) =
    copy(labels = labels.asScala.toMap)
  def withLabel(key: String, value: String) =
    copy(labels = labels + (key -> value))
  def withoutLabels =
    copy(labels = Map.empty)

  def withJsonPayload[U](jsonPayload: U) =
    copy(jsonPayload = jsonPayload)
}

object LogEntry {

  /**
   * An individual entry in a log.
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry Cloud Logging Reference]]
   *
   * @param jsonPayload The log entry payload, represented as a structure that is expressed as a JSON object.
   * @tparam T The data model for the `jsonPayload`
   * @return a [[LogEntry]]
   */
  def apply[T](jsonPayload: T): LogEntry[T] =
    apply(None, None, None, None, None, Map.empty, jsonPayload)

  /**
   * Java API: An individual entry in a log.
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry Cloud Logging Reference]]
   *
   * @param jsonPayload The log entry payload, represented as a structure that is expressed as a JSON object.
   * @tparam T The data model for the `jsonPayload`
   * @return a [[LogEntry]]
   */
  def create[T](jsonPayload: T): LogEntry[T] = apply(jsonPayload)

  /**
   * Java API: An individual entry in a log.
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry Cloud Logging Reference]]
   *
   * @param logName The resource name of the log to which this log entry belongs.
   * @param resource The monitored resource that produced this log entry.
   * @param timestamp The time the event described by the log entry occurred.
   * @param severity The severity of the log entry.
   * @param insertId A unique identifier for the log entry.
   * @param labels A set of user-defined (key, value) data that provides additional information about the log entry.
   * @param jsonPayload The log entry payload, represented as a structure that is expressed as a JSON object.
   * @tparam T The data model for the `jsonPayload`
   * @return a [[LogEntry]]
   */
  def create[T](logName: util.Optional[String],
                resource: util.Optional[MonitoredResource],
                timestamp: util.Optional[Instant],
                severity: util.Optional[LogSeverity],
                insertId: util.Optional[String],
                labels: util.Map[String, String],
                jsonPayload: T): LogEntry[T] =
    apply(logName.asScala.map(Path(_)),
          resource.asScala,
          timestamp.asScala,
          severity.asScala,
          insertId.asScala,
          labels.asScala.toMap,
          jsonPayload)

  private def orderBySeverity[T]: Ordering[LogEntry[T]] =
    Ordering.by[LogEntry[T], LogSeverity](_.severity.getOrElse(LogSeverity.Default))

  private def partialOrderByTimestamp[T]: PartialOrdering[LogEntry[T]] = new PartialOrdering[LogEntry[T]] {

    override def tryCompare(x: LogEntry[T], y: LogEntry[T]): Option[Int] =
      for {
        x <- x.timestamp
        y <- y.timestamp
      } yield Ordering[Instant].compare(x, y)

    override def lteq(x: LogEntry[T], y: LogEntry[T]): Boolean = {
      val lteq = for {
        x <- x.timestamp
        y <- y.timestamp
      } yield Ordering[Instant].lteq(x, y)
      lteq.getOrElse(false)
    }

  }

  private[logging] def ordering[T](prioritizeLatest: Boolean): Ordering[LogEntry[T]] = new Ordering[LogEntry[T]] {
    private val primary = orderBySeverity[T]
    private val secondary =
      if (prioritizeLatest)
        partialOrderByTimestamp[T]
      else
        partialOrderByTimestamp[T].reverse

    override def compare(x: LogEntry[T], y: LogEntry[T]): Int = primary.compare(x, y) match {
      case 0 => secondary.tryCompare(x, y).getOrElse(0)
      case z => z
    }
  }

  private implicit object instantWriter extends JsonWriter[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)
  }

  implicit def writer[T](implicit writer: JsonWriter[T]): JsonWriter[LogEntry[T]] = {
    implicit val instantFormat = lift(instantWriter)
    implicit val format = lift(writer)
    jsonFormat7(LogEntry[T])
  }
}

/**
 * The severity of the event described in a log entry, expressed as one of the standard severity levels listed below.
 * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#logseverity Cloud Logging Reference]]
 */
@DoNotInherit
final case class LogSeverity private (asInt: Int)

object LogSeverity {

  /**
   * Parses the severity from a [[String]]
   */
  def apply(severity: String): LogSeverity = Helpers.toRootLowerCase(severity) match {
    case "default" => Default
    case "debug" => Debug
    case "info" => Info
    case "notice" => Notice
    case "warning" => Warning
    case "error" => Error
    case "critical" => Critical
    case "alert" => Alert
    case "emergency" => Emergency
    case _ => throw new IllegalArgumentException(s"Unrecognized severity: $severity")
  }

  /**
   * Creates a severity at the given level
   */
  def apply(severity: Int) = new LogSeverity(severity)

  def unapply(severity: LogSeverity): Some[Int] = Some(severity.asInt)

  private def unapply(severity: String): Option[LogSeverity] = Try(apply(severity)).toOption

  /**
   * The log entry has no assigned severity level.
   */
  val Default = LogSeverity(0)

  /**
   * Java API: The log entry has no assigned severity level.
   */
  def default: LogSeverity = Default

  /**
   * Debug or trace information.
   */
  val Debug = LogSeverity(100)

  /**
   * Java API: Debug or trace information.
   */
  def debug: LogSeverity = Debug

  /**
   * Routine information, such as ongoing status or performance.
   */
  val Info = LogSeverity(200)

  /**
   * Java API: Routine information, such as ongoing status or performance.
   */
  def info: LogSeverity = Info

  /**
   * Normal but significant events, such as start up, shut down, or a configuration change.
   */
  val Notice = LogSeverity(300)

  /**
   * Java API: Normal but significant events, such as start up, shut down, or a configuration change.
   */
  def notice: LogSeverity = Notice

  /**
   * Warning events might cause problems.
   */
  val Warning = LogSeverity(400)

  /**
   * Java API: Warning events might cause problems.
   */
  def warning: LogSeverity = Warning

  /**
   * Error events are likely to cause problems.
   */
  val Error = LogSeverity(500)

  /**
   * Java API: Error events are likely to cause problems.
   */
  def error: LogSeverity = Error

  /**
   * Critical events cause more severe problems or outages.
   */
  val Critical = LogSeverity(600)

  /**
   * Java API: Critical events cause more severe problems or outages.
   */
  def critical: LogSeverity = Critical

  /**
   * A person must take an action immediately.
   */
  val Alert = LogSeverity(700)

  /**
   * Java API: A person must take an action immediately.
   */
  def alert: LogSeverity = Alert

  /**
   * One or more systems are unusable.
   */
  val Emergency = LogSeverity(800)

  /**
   * Java API: One or more systems are unusable.
   */
  def emergency: LogSeverity = Emergency

  implicit val ordering: Ordering[LogSeverity] = Ordering.Int.on(_.asInt)

  implicit object format extends JsonFormat[LogSeverity] {
    override def read(json: JsValue): LogSeverity = json match {
      case JsNumber(severity) if severity.isValidInt => LogSeverity(severity.toIntExact)
      case JsString(LogSeverity(severity)) => severity
      case x => deserializationError("Expected LogSeverity as JsNumber or JsString, but got " + x)
    }
    override def write(obj: LogSeverity): JsValue = JsNumber(obj.asInt)
  }
}
