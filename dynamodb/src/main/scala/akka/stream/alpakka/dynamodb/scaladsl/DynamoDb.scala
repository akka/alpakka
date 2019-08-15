/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPaginatedOp}
import akka.stream.scaladsl.{Flow, Sink, Source}
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.Future

/**
 * Factory of DynamoDb Akka Stream operators.
 */
object DynamoDb {

  /**
   * Create a Flow that emits a response for every request to DynamoDB.
   */
  def flow[In <: DynamoDbRequest, Out <: DynamoDbResponse](
      parallelism: Int
  )(implicit client: DynamoDbAsyncClient, operation: AwsOp[In, Out]): Flow[In, Out, NotUsed] =
    Flow[In].mapAsync(parallelism)(operation.execute(_))

  /**
   * Create a Source that will emit potentially multiple responses for a given request.
   */
  def source[In <: DynamoDbRequest, Out <: DynamoDbResponse, Pub <: SdkPublisher[Out]](
      request: In
  )(implicit client: DynamoDbAsyncClient, operation: AwsPaginatedOp[In, Out, Pub]): Source[Out, NotUsed] =
    Source.fromPublisher(operation.publisher(request))

  /**
   * Create a Future that will be completed with a response to a given request.
   */
  def single[In <: DynamoDbRequest, Out <: DynamoDbResponse](
      request: In
  )(implicit client: DynamoDbAsyncClient, operation: AwsOp[In, Out], mat: Materializer): Future[Out] =
    Source.single(request).via(flow(1)).runWith(Sink.head)
}
