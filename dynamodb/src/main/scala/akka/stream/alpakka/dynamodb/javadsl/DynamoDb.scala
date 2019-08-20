/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.{scaladsl, DynamoDbOp, DynamoDbPaginatedOp}
import akka.stream.javadsl.{Flow, Sink, Source}
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

/**
 * Factory of DynamoDb Akka Stream operators.
 */
object DynamoDb {

  /**
   * Create a Flow that emits a response for every request.
   */
  def flow[In <: DynamoDbRequest, Out <: DynamoDbResponse](client: DynamoDbAsyncClient,
                                                           operation: DynamoDbOp[In, Out],
                                                           parallelism: Int): Flow[In, Out, NotUsed] =
    scaladsl.DynamoDb.flow(parallelism)(client, operation).asJava

  /**
   * Create a Source that will emit potentially multiple responses for a given request.
   *
   */
  def source[In <: DynamoDbRequest, Out <: DynamoDbResponse, Pub <: SdkPublisher[Out]](
      client: DynamoDbAsyncClient,
      operation: DynamoDbPaginatedOp[In, Out, Pub],
      request: In
  ): Source[Out, NotUsed] =
    scaladsl.DynamoDb.source(request)(client, operation).asJava

  /**
   * Create a CompletionStage that will be completed with a response to a given request.
   */
  def single[In <: DynamoDbRequest, Out <: DynamoDbResponse](client: DynamoDbAsyncClient,
                                                             operation: DynamoDbOp[In, Out],
                                                             request: In,
                                                             mat: Materializer): CompletionStage[Out] =
    Source.single(request).via(flow(client, operation, 1)).runWith(Sink.head(), mat)
}
