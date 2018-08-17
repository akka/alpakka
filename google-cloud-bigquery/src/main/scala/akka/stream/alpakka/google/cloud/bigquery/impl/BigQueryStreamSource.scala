/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl

import java.net.URLEncoder

import akka.NotUsed
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.stream._
import akka.stream.alpakka.google.cloud.bigquery.impl.pagetoken.{AddPageToken, EndOfStreamDetector}
import akka.stream.alpakka.google.cloud.bigquery.impl.parser.Parser
import akka.stream.alpakka.google.cloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.google.cloud.bigquery.impl.sendrequest.SendRequestWithOauthHandling
import akka.stream.alpakka.google.cloud.bigquery.impl.util.{Delay, FlowInitializer}
import akka.stream.scaladsl.{GraphDSL, Source, Zip}
import spray.json.JsObject

import scala.concurrent.ExecutionContext

object BigQueryStreamSource {

  private[bigquery] def apply[T](httpRequest: HttpRequest,
                                 parserFn: JsObject => Option[T],
                                 googleSession: GoogleSession,
                                 http: HttpExt)(
      implicit mat: Materializer
  ): Source[T, NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      implicit val ec: ExecutionContext = mat.executionContext

      val in = builder.add(Source.repeat(httpRequest))
      val requestSender = builder.add(SendRequestWithOauthHandling(googleSession, http))
      val parser = builder.add(Parser(parserFn))
      val endOfStreamDetector = builder.add(EndOfStreamDetector())
      val flowInitializer = builder.add(FlowInitializer((false, PagingInfo(None, None))))
      val delay = builder.add(Delay[(Boolean, PagingInfo)](_._1, 60))
      val zip = builder.add(Zip[HttpRequest, (Boolean, PagingInfo)]())
      val addPageTokenNode = builder.add(AddPageToken())

      in ~> zip.in0
      requestSender ~> parser.in
      parser.out1 ~> endOfStreamDetector
      endOfStreamDetector ~> delay
      delay ~> flowInitializer
      flowInitializer ~> zip.in1
      zip.out ~> addPageTokenNode
      addPageTokenNode ~> requestSender

      SourceShape(parser.out0)

    /*
        +--------+           +------------+          +-------+         +------+
        |Request |           |AddPageToken|          |Request|         |Parser|
        |Repeater+---------->+            +--------->+Sender +-------->+      +-----+(response)+----->
        |        |           |            |          |       |         |      |
        +--------+           +-----+------+          +-------+         +---+--+
                                   ^                                       |
                                   |                                       |
                                   |     +-----------+                     |
                                   |     |   Flow    |                     |
                                   +<----+Initializer|                     |
                                   |     | (single)  |                     |
                                   |     +-----------+                     |
                                   |                                       |
                                   |       +-----+       +-----------+     |
                                   |       |Delay|       |EndOfStream|     |
                                   +-------+     +<------+  Detector +<----+
                                           |     |       |           |
                                           +-----+       +-----------+
     */
    })

  private def addPageToken(request: HttpRequest, pagingInfo: PagingInfo, retry: Boolean): HttpRequest = {
    val req = if (retry) {
      request
    } else {
      request.copy(
        uri = request.uri
          .withQuery(Uri.Query(pagingInfo.pageToken.map(token => s"pageToken=${URLEncoder.encode(token, "utf-8")}")))
      )
    }

    pagingInfo.jobId match {
      case Some(id) =>
        val getReq = req.copy(method = HttpMethods.GET, entity = HttpEntity.Empty)
        getReq.copy(uri = req.uri.withPath(req.uri.path ?/ id))
      case None => req
    }
  }
}
