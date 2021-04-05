/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.model

import spray.json.JsonFormat
import spray.json.DefaultJsonProtocol._

import java.util
import scala.collection.JavaConverters._

/**
 * An object representing a resource that can be used for monitoring, logging, billing, or other purposes.
 * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/MonitoredResource Cloud Logging Reference]]
 *
 * @param `type` The monitored resource type.
 * @param labels Values for all of the labels listed in the associated monitored resource descriptor.
 */
final case class MonitoredResource private (`type`: String, labels: Map[String, String]) {

  def getType = `type`
  def getLabels = labels.asJava

  def withType(`type`: String) =
    copy(`type` = `type`)

  def withLabels(labels: Map[String, String]) =
    copy(labels = labels)
  def withLabels(labels: util.Map[String, String]) =
    copy(labels = labels.asScala.toMap)
  def withLabel(key: String, value: String) =
    copy(labels = labels + (key -> value))
  def withoutLabels =
    copy(labels = Map.empty)
}

object MonitoredResource {

  /**
   * Java API: An object representing a resource that can be used for monitoring, logging, billing, or other purposes.
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/MonitoredResource Cloud Logging Reference]]
   *
   * @param `type` The monitored resource type.
   * @param labels Values for all of the labels listed in the associated monitored resource descriptor.
   * @return a [[MonitoredResource]]
   */
  def create(`type`: String, labels: util.Map[String, String]) =
    MonitoredResource(`type`, labels.asScala.toMap)

  /**
   * A resource type used to indicate that a log is not associated with any specific resource.
   */
  val Global = MonitoredResource("global", Map.empty)

  /**
   * Java API: A resource type used to indicate that a log is not associated with any specific resource.
   */
  def global = Global

  implicit val format: JsonFormat[MonitoredResource] = jsonFormat2(apply)
}
