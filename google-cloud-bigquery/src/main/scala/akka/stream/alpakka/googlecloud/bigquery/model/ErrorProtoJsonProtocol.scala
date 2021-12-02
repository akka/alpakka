/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.github.ghik.silencer.silent
import spray.json.JsonFormat

import java.util
import scala.compat.java8.OptionConverters._

/**
 * ErrorProto model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/ErrorProto BigQuery reference]]
 *
 * @param reason a short error code that summarizes the error
 * @param location specifies where the error occurred, if present
 * @param message A human-readable description of the error
 */
final case class ErrorProto private (reason: Option[String], location: Option[String], message: Option[String]) {

  @silent("never used")
  @JsonCreator
  private def this(@JsonProperty(value = "reason") reason: String,
                   @JsonProperty("location") location: String,
                   @JsonProperty(value = "message") message: String) =
    this(Option(reason), Option(location), Option(message))

  def getReason = reason.asJava
  def getLocation = location.asJava
  def getMessage = message.asJava

  def withReason(reason: Option[String]) =
    copy(reason = reason)
  def withReason(reason: util.Optional[String]) =
    copy(reason = reason.asScala)

  def withLocation(location: Option[String]) =
    copy(location = location)
  def withLocation(location: util.Optional[String]) =
    copy(location = location.asScala)

  def withMessage(message: Option[String]) =
    copy(message = message)
  def withMessage(message: util.Optional[String]) =
    copy(message = message.asScala)
}

object ErrorProto {

  /**
   * Java API: ErrorProto model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/ErrorProto BigQuery reference]]
   *
   * @param reason a short error code that summarizes the error
   * @param location specifies where the error occurred, if present
   * @param message A human-readable description of the error
   * @return an [[ErrorProto]]
   */
  def create(reason: util.Optional[String], location: util.Optional[String], message: util.Optional[String]) =
    ErrorProto(reason.asScala, location.asScala, message.asScala)

  implicit val format: JsonFormat[ErrorProto] = jsonFormat3(apply)
}
