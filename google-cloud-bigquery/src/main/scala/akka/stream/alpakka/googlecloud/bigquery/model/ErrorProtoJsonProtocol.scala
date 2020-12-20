/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import spray.json.{DefaultJsonProtocol, JsonFormat}

object ErrorProtoJsonProtocol extends DefaultJsonProtocol {

  final case class ErrorProto(reason: String, location: Option[String], message: String)

  implicit val format: JsonFormat[ErrorProto] = jsonFormat3(ErrorProto)
}
