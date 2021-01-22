/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryApiJsonProtocol._
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import spray.json.JsonFormat

import java.util
import scala.compat.java8.OptionConverters._

object ErrorProtoJsonProtocol {

  /**
   * ErrorProto model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/ErrorProto BigQuery reference]]
   *
   * @param reason a short error code that summarizes the error
   * @param location specifies where the error occurred, if present
   * @param message A human-readable description of the error
   */
  final case class ErrorProto(reason: String, location: Option[String], message: String) {

    @JsonCreator
    private def this(@JsonProperty(value = "reason", required = true) reason: String,
                     @JsonProperty("location") location: String,
                     @JsonProperty(value = "message", required = true) message: String) =
      this(reason, Option(location), message)

    def getReason = reason
    def getLocation = location.asJava
    def getMessage = message

    def withReason(reason: String) =
      copy(reason = reason)
    def withLocation(location: util.Optional[String]) =
      copy(location = location.asScala)
    def withMessage(message: String) =
      copy(message = message)
  }

  /**
   * ErrorProto model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/ErrorProto BigQuery reference]]
   *
   * @param reason a short error code that summarizes the error
   * @param location specifies where the error occurred, if present
   * @param message A human-readable description of the error
   * @return an [[ErrorProto]]
   */
  def createErrorProto(reason: String, location: util.Optional[String], message: String) =
    ErrorProto(reason, location.asScala, message)

  implicit val format: JsonFormat[ErrorProto] = jsonFormat3(ErrorProto)
}
