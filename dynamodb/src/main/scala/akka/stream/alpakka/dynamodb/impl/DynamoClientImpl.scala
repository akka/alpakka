/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.MediaType.NotCompressible
import akka.http.scaladsl.model.{ContentType, MediaType}
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.impl.AwsClient.{AwsConnect, AwsRequestMetadata}
import com.amazonaws.AmazonServiceException
import com.amazonaws.http.HttpResponseHandler

import scala.concurrent.ExecutionContextExecutor

class DynamoClientImpl(
    val settings: DynamoSettings,
    val errorResponseHandler: HttpResponseHandler[AmazonServiceException]
)(implicit protected val system: ActorSystem, implicit protected val materializer: Materializer)
    extends AwsClient[DynamoSettings] {

  override protected val service = "dynamodb"
  override protected val defaultContentType =
    ContentType.Binary(MediaType.customBinary("application", "x-amz-json-1.0", NotCompressible))
  override protected implicit val ec: ExecutionContextExecutor = system.dispatcher

  override protected val connection: AwsConnect =
    if (settings.port == 443)
      Http().cachedHostConnectionPoolHttps[AwsRequestMetadata](settings.host)
    else
      Http().cachedHostConnectionPool[AwsRequestMetadata](settings.host, settings.port)

}
