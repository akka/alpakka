/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl

import akka.NotUsed
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.HttpRequest
import akka.stream._
import akka.stream.alpakka.google.cloud.bigquery.impl.pagetoken.AddPageToken
import akka.stream.alpakka.google.cloud.bigquery.impl.parser.Parser
import akka.stream.alpakka.google.cloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.google.cloud.bigquery.impl.sendrequest.SendRequestWithOauthHandling
import akka.stream.alpakka.google.cloud.bigquery.impl.util.FlowInitializer
import akka.stream.scaladsl.{Flow, GraphDSL, Source, Zip}
import spray.json.JsObject

import scala.concurrent.ExecutionContext

object BigQueryStreamSource {

  private[bigquery] def apply[T](httpRequest: HttpRequest,
                                 parserFn: JsObject => T,
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
      val endOfStreamDetector = builder.add(Flow[PagingInfo].takeWhile { _.pageToken.isDefined })
      val flowInitializer = builder.add(FlowInitializer(PagingInfo(None, None)))
      val zip = builder.add(Zip[HttpRequest, PagingInfo]())
      val addPageTokenNode = builder.add(AddPageToken())

      in ~> zip.in0
      requestSender ~> parser.in
      parser.out1 ~> endOfStreamDetector
      endOfStreamDetector ~> flowInitializer
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
                                   |                     +-----------+     |
                                   |                     |EndOfStream|     |
                                   +---------------------+  Detector +<----+
                                                         |           |
                                                         +-----------+
     */
    })
}
