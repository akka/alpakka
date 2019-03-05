/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.MediaType.NotCompressible
import akka.http.scaladsl.model.{ContentType, MediaType}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.DynamoSettings
import akka.stream.alpakka.dynamodb.impl.AwsClient.{AwsConnect, AwsRequestMetadata}
import com.amazonaws.AmazonServiceException
import com.amazonaws.http.HttpResponseHandler

import scala.concurrent.ExecutionContextExecutor

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] class DynamoClientImpl(
    val settings: DynamoSettings,
    val errorResponseHandler: HttpResponseHandler[AmazonServiceException]
)(implicit protected val system: ActorSystem, implicit protected val materializer: Materializer)
    extends AwsClient[DynamoSettings] {

  override protected val service = "dynamodb"
  override protected val defaultContentType =
    ContentType.Binary(MediaType.customBinary("application", "x-amz-json-1.0", NotCompressible))
  override protected implicit val ec: ExecutionContextExecutor = system.dispatcher

  override protected val connection: AwsConnect = {
    val poolSettings = ConnectionPoolSettings(system)
      .withMaxConnections(settings.parallelism)
      .withMaxOpenRequests(settings.maxOpenRequests.getOrElse(settings.parallelism))
    if (settings.tls)
      Http().cachedHostConnectionPoolHttps[AwsRequestMetadata](settings.host, settings = poolSettings)
    else
      Http().cachedHostConnectionPool[AwsRequestMetadata](settings.host, settings.port, settings = poolSettings)
  }

  override protected def url: String = if (settings.tls) s"https://${settings.host}/" else s"http://${settings.host}/"
}
