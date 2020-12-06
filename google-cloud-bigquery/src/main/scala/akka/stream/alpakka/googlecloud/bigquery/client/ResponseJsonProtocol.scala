/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.client

import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import java.util.Optional

object ResponseJsonProtocol extends DefaultJsonProtocol {

  final case class Response(jobReference: Option[JobReference],
                            pageToken: Option[String],
                            nextPageToken: Option[String],
                            jobComplete: Option[Boolean])

  /**
   * Java API
   */
  def createResponse(jobReference: Optional[JobReference],
                     pageToken: Optional[String],
                     nextPageToken: Optional[String],
                     jobComplete: Optional[java.lang.Boolean]): Response = {
    Response(jobReference.asScala, pageToken.asScala, nextPageToken.asScala, jobComplete.asScala.map(_.booleanValue()))
  }

  final case class JobReference(jobId: Option[String])

  /**
   * Java API
   */
  def createJobReference(jobId: Optional[String]): JobReference = JobReference(jobId.asScala)

  implicit val jobReferenceFormat: JsonFormat[JobReference] = jsonFormat1(JobReference)
  implicit val responseFormat: RootJsonFormat[Response] = jsonFormat4(Response)

  final case class ResponseRows[T](rows: Option[Seq[T]])

  /**
   * Java API
   */
  def createResponseRows[T](rows: Optional[java.util.List[T]]): ResponseRows[T] = {
    ResponseRows(rows.asScala.map(_.asScala.toSeq))
  }

  implicit def responseRowsFormat[T: JsonFormat]: RootJsonFormat[ResponseRows[T]] = jsonFormat1(ResponseRows[T])

}
