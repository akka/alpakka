/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.paginated

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal, Unmarshaller}
import akka.stream.alpakka.googlecloud.bigquery.BigQueryException._
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.impl.util.Delay
import akka.stream.alpakka.googlecloud.bigquery.model.ResponseMetadataJsonProtocol.ResponseMetadata
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQuerySettings}
import akka.stream.scaladsl.{Flow, GraphDSL, Source, Zip}
import akka.stream.{FlowShape, Graph, SourceShape}

import scala.concurrent.Future

@InternalApi
private[bigquery] object PaginatedRequest {

  def apply[Out, Json](httpRequest: HttpRequest)(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
      responseUnmarshaller: Unmarshaller[Json, ResponseMetadata],
      unmarshaller: Unmarshaller[Json, Out]
  ): Source[Out, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val materializer = mat
        implicit val system = mat.system
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)

        Source.fromGraph {
          GraphDSL.create() { implicit builder =>
            import GraphDSL.Implicits._

            val requestRepeater = builder.add(Source.repeat(httpRequest))
            val requestSender = builder.add(Flow[HttpRequest].mapAsync(1)(sendRequest[Json]))
            val parser = builder.add(Parser[Out, Json])
            val endOfStreamDetector = builder.add(EndOfStreamDetector)
            val flowInitializer = builder.add(FlowInitializer)
            val delay = builder.add(delayIfIncomplete)
            val zip = builder.add(Zip[HttpRequest, Option[PagingInfo]]())
            val addPageToken = builder.add(AddPageToken())

            requestRepeater ~> zip.in0
            zip.out ~> addPageToken ~> requestSender ~> parser.in
            zip.in1 <~ flowInitializer <~ delay <~ endOfStreamDetector <~ parser.out1

            SourceShape(parser.out0)
          }
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def sendRequest[Json](request: HttpRequest)(implicit system: ClassicActorSystemProvider,
                                                      settings: BigQuerySettings,
                                                      unmarshaller: FromEntityUnmarshaller[Json]): Future[Json] =
    BigQueryHttp()
      .retryRequestWithOAuth(request)
      .flatMap { response =>
        Unmarshal(response.entity).to[Json]
      }(system.classicSystem.dispatcher)

  private[paginated] val FlowInitializer = Flow[PagingInfo].map(Some(_)).prepend(Source.single(None))

  private def delayIfIncomplete(
      implicit settings: BigQuerySettings
  ): Graph[FlowShape[PagingInfo, PagingInfo], NotUsed] = {
    Delay[PagingInfo](settings.retrySettings)(_.jobInfo.exists(!_.jobComplete))
  }

  private[paginated] val EndOfStreamDetector =
    Flow[PagingInfo].takeWhile {
      case PagingInfo(jobInfo, pageToken) =>
        jobInfo.exists(!_.jobComplete) | pageToken.isDefined
    }
}
