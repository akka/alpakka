/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream._
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryConfig, BigQueryJsonProtocol}
import akka.stream.alpakka.googlecloud.bigquery.impl.pagetoken.{AddPageToken, EndOfStreamDetector}
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.googlecloud.bigquery.impl.sendrequest.SendRequestWithOauthHandling
import akka.stream.alpakka.googlecloud.bigquery.impl.util.{Delay, FlowInitializer, OnFinishCallback}
import akka.stream.scaladsl.{GraphDSL, Source, Zip}

import scala.concurrent.ExecutionContext

@InternalApi
private[bigquery] object BigQueryStreamSource {

  def callbackConverter(onFinishCallback: PagingInfo => NotUsed): ((Boolean, PagingInfo)) => Unit =
    (t: (Boolean, PagingInfo)) => { onFinishCallback(t._2); {} }

  def apply[J, T](httpRequest: HttpRequest,
                  onFinishCallback: PagingInfo => NotUsed,
                  projectConfig: BigQueryConfig,
                  http: HttpExt)(
      implicit mat: Materializer,
      system: ActorSystem,
      jsonUnmarshaller: FromEntityUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
      unmarshaller: Unmarshaller[J, T]
  ): Source[T, NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      implicit val ec: ExecutionContext = mat.executionContext

      val in = builder.add(Source.repeat(httpRequest))
      val requestSender = builder.add(SendRequestWithOauthHandling(projectConfig, http))
      val parser = builder.add(Parser[J, T])
      val uptreamFinishHandler =
        builder.add(OnFinishCallback[(Boolean, PagingInfo)](callbackConverter(onFinishCallback)))
      val endOfStreamDetector = builder.add(EndOfStreamDetector())
      val flowInitializer = builder.add(FlowInitializer((false, PagingInfo(None, None))))
      val delay = builder.add(Delay[(Boolean, PagingInfo)](_._1, 60))
      val zip = builder.add(Zip[HttpRequest, (Boolean, PagingInfo)]())
      val addPageTokenNode = builder.add(AddPageToken())

      in ~> zip.in0
      requestSender ~> parser.in
      parser.out1 ~> uptreamFinishHandler
      uptreamFinishHandler ~> endOfStreamDetector
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
                                     |     +-----------+                +----+------+
                                     |     |   Flow    |                | UpStream  |
                                     +<----+Initializer|                |  Finish   |
                                     |     | (single)  |                |  Handler  |
                                     |     +-----------+                +----+------+
                                     |                                       |
                                     |       +-----+       +-----------+     |
                                     |       |Delay|       |EndOfStream|     |
                                     +-------+     +<------+  Detector +<----+
                                             |     |       |           |
                                             +-----+       +-----------+
     */
    })
}
