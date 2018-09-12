/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.parser

import akka.NotUsed
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{FanOutShape2, FlowShape, Graph, Materializer}
import spray.json._

import scala.concurrent.ExecutionContext

object Parser {
  final case class PagingInfo(pageToken: Option[String], jobId: Option[String])

  object ParserJsonProtocol extends DefaultJsonProtocol {

    case class Response(jobReference: Option[JobReference], pageToken: Option[String], nextPageToken: Option[String])

    case class JobReference(jobId: Option[String])

    implicit val jobReferenceFormat: RootJsonFormat[JobReference] = jsonFormat1(JobReference)
    implicit val responseFormat: RootJsonFormat[Response] = jsonFormat3(Response)
  }

  def apply[T](parseFunction: JsObject => T)(
      implicit materializer: Materializer,
      ec: ExecutionContext
  ): Graph[FanOutShape2[HttpResponse, T, PagingInfo], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val bodyJsonParse: FlowShape[HttpResponse, JsObject] = builder.add(Flow[HttpResponse].mapAsync(1)(parseHttpBody(_)))

    val parseMap = builder.add(Flow[JsObject].map(parseFunction(_)))
    val pageInfoProvider = builder.add(Flow[JsObject].map(getPageInfo))

    val broadcast1 = builder.add(Broadcast[JsObject](2))

    bodyJsonParse ~> broadcast1

    broadcast1.out(0) ~> parseMap
    broadcast1.out(1) ~> pageInfoProvider

    new FanOutShape2(bodyJsonParse.in, parseMap.out, pageInfoProvider.out)
  }

  private def parseHttpBody[T](response: HttpResponse)(implicit materializer: Materializer, ec: ExecutionContext) =
    Unmarshal(response.entity)
      .to[String]
      .map {
        case "" => JsObject()
        case nonEmptyString => nonEmptyString.parseJson.asJsObject
      }

  private def getPageInfo[T](jsObject: JsObject): PagingInfo = {
    import ParserJsonProtocol._

    val response = jsObject.convertTo[Response]

    val pageToken = response.pageToken orElse response.nextPageToken
    val jobId = response.jobReference.flatMap(_.jobId)

    PagingInfo(pageToken, jobId)
  }
}
