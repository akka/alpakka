/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

object BigQueryJsonProtocol extends DefaultJsonProtocol {

  final case class Response(jobReference: Option[JobReference],
                            pageToken: Option[String],
                            nextPageToken: Option[String],
                            jobComplete: Option[Boolean])

  final case class JobReference(jobId: Option[String])

  implicit val jobReferenceFormat: JsonFormat[JobReference] = jsonFormat1(JobReference)
  implicit val responseFormat: RootJsonFormat[Response] = jsonFormat4(Response)

  final case class ResponseRows[T](rows: Option[Seq[T]])

  implicit def responseRowsFormat[T: JsonFormat]: RootJsonFormat[ResponseRows[T]] = jsonFormat1(ResponseRows[T])

}
