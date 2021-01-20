/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryApiJsonProtocol._
import spray.json.JsonFormat

import java.util
import scala.compat.java8.OptionConverters._

object ErrorProtoJsonProtocol {

  final case class ErrorProto(reason: String, location: Option[String], message: String) {

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
   * Java API
   */
  def createErrorProto(reason: String, location: util.Optional[String], message: String) =
    ErrorProto(reason, location.asScala, message)

  implicit val format: JsonFormat[ErrorProto] = jsonFormat3(ErrorProto)
}
