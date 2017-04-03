/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.MediaType.NotCompressible
import akka.http.scaladsl.model.{ContentType, MediaType}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.impl.AwsClient.{AwsConnect, AwsRequestMetadata}
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.AmazonServiceException
import com.amazonaws.http.HttpResponseHandler

class DynamoClientImpl(
    val settings: DynamoSettings,
    val errorResponseHandler: HttpResponseHandler[AmazonServiceException]
)(implicit protected val system: ActorSystem, implicit protected val materializer: ActorMaterializer)
    extends AwsClient[DynamoSettings] {

  override protected val service = "dynamodb"
  override protected val defaultContentType =
    ContentType.Binary(MediaType.customBinary("application", "x-amz-json-1.0", NotCompressible))
  override protected implicit val ec = system.dispatcher

  override protected val connection: AwsConnect =
    if (settings.port == 443)
      Http().cachedHostConnectionPoolHttps[AwsRequestMetadata](settings.host)(materializer)
    else
      Http().cachedHostConnectionPool[AwsRequestMetadata](settings.host, settings.port)(materializer)

  def single(op: AwsOp) = Source.single(op).via(flow).map(_.asInstanceOf[op.B]).runWith(Sink.head)

}
