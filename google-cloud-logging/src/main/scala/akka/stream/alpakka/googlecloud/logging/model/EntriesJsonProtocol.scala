/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.model

import akka.http.scaladsl.model.Uri.Path
import akka.stream.alpakka.googlecloud.logging.model.PathJsonProtocol._
import spray.json.{JsonWriter, RootJsonWriter}
import spray.json.DefaultJsonProtocol._

import java.lang
import java.util
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable.Seq
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

/**
 * `entries.write` request model.
 * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write#request-body Cloud Logging Reference]]
 *
 * @param logName A default log resource name that is assigned to all log entries in `entries` that do not specify a value for `logName`.
 * @param resource A default monitored resource object that is assigned to all log entries in `entries` that do not specify a value for `resource`.
 * @param labels Default labels that are added to the `labels` field of all log entries in `entries`. If a log entry already has a label with the same key as a label in this parameter, then the log entry's label is not changed.
 * @param entries The log entries to send to Logging.
 * @param partialSuccess Whether valid entries should be written even if some other entries fail due to `INVALID_ARGUMENT` or `PERMISSION_DENIED` errors.
 * @param dryRun If true, the request should expect normal response, but the entries won't be persisted nor exported.
 * @tparam T The data model for the `jsonPayload` of `entries`.
 */
final case class WriteEntriesRequest[+T] private (logName: Option[Path],
                                                  resource: Option[MonitoredResource],
                                                  labels: Map[String, String],
                                                  entries: Seq[LogEntry[T]],
                                                  partialSuccess: Option[Boolean],
                                                  dryRun: Option[Boolean]) {

  def getLogName = logName.map(_.asString).asJava
  def getResource = resource.asJava
  def getLabels = labels.asJava
  def getEntries: util.List[LogEntry[T @uncheckedVariance]] = entries.asJava
  def getPartialSuccess = partialSuccess.map(lang.Boolean.valueOf).asJava
  def getDryRun = dryRun.map(lang.Boolean.valueOf).asJava

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

  def withLabels(labels: Map[String, String]) =
    copy(labels = labels)
  def withLabels(labels: util.Map[String, String]) =
    copy(labels = labels.asScala.toMap)
  def withLabel(key: String, value: String) =
    copy(labels = labels + (key -> value))
  def withoutLabels =
    copy(labels = Map.empty)

  def withEntries[U](entries: Seq[LogEntry[U]]) =
    copy(entries = entries)
  def withEntries[U](entries: util.List[LogEntry[U]]) =
    copy(entries = entries.asScala.toVector)
  def withEntry[U >: T](entry: LogEntry[U]) =
    copy(entries = entries :+ entry)
  def withoutEntries =
    copy(entries = Seq.empty)

  def withPartialSuccess(partialSuccess: Boolean) =
    copy(partialSuccess = Some(partialSuccess))
  def withoutPartialSuccess =
    copy(partialSuccess = None)

  def withDryRun(dryRun: Boolean) =
    copy(dryRun = Some(dryRun))
  def withoutDryRun =
    copy(dryRun = None)
}

object WriteEntriesRequest {

  /**
   * `entries.write` request model.
   *
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write#request-body Cloud Logging Reference]]
   * @tparam T The data model for the `jsonPayload` of `entries`.
   * @return an [[WriteEntriesRequest]]
   */
  def apply[T](): WriteEntriesRequest[T] =
    new WriteEntriesRequest[T](None, None, Map.empty, Seq.empty, None, None)

  /**
   * Java API: `entries.write` request model.
   *
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write#request-body Cloud Logging Reference]]
   * @tparam T The data model for the `jsonPayload` of `entries`.
   * @return an [[WriteEntriesRequest]]
   */
  def create[T](): WriteEntriesRequest[T] = apply[T]()

  /**
   * Java API: `entries.write` request model.
   *
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write#request-body Cloud Logging Reference]]
   * @param logName A default log resource name that is assigned to all log entries in `entries` that do not specify a value for `logName`.
   * @param resource A default monitored resource object that is assigned to all log entries in `entries` that do not specify a value for `resource`.
   * @param labels Default labels that are added to the `labels` field of all log entries in `entries`. If a log entry already has a label with the same key as a label in this parameter, then the log entry's label is not changed.
   * @param entries The log entries to send to Logging.
   * @param partialSuccess Whether valid entries should be written even if some other entries fail due to `INVALID_ARGUMENT` or `PERMISSION_DENIED` errors.
   * @param dryRun If true, the request should expect normal response, but the entries won't be persisted nor exported.
   * @tparam T The data model for the `jsonPayload` of `entries`.
   * @return an [[WriteEntriesRequest]]
   */
  def create[T](logName: util.Optional[String],
                resource: util.Optional[MonitoredResource],
                labels: util.Map[String, String],
                entries: util.List[LogEntry[T]],
                partialSuccess: util.Optional[lang.Boolean],
                dryRun: util.Optional[lang.Boolean]): WriteEntriesRequest[T] =
    apply(
      logName.asScala.map(Path(_)),
      resource.asScala,
      labels.asScala.toMap,
      entries.asScala.toVector,
      partialSuccess.asScala.map(_.booleanValue),
      dryRun.asScala.map(_.booleanValue)
    )

  implicit def writer[T](implicit writer: JsonWriter[T]): RootJsonWriter[WriteEntriesRequest[T]] = {
    implicit val format = lift(writer)
    implicit val logEntryFormat = lift(LogEntry.writer[T])
    jsonFormat6(WriteEntriesRequest[T])
  }
}
