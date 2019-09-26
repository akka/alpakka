/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.{DynamoDbOp, DynamoDbPaginatedOp}
import akka.stream.scaladsl.{Flow, FlowWithContext, Sink, Source}
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * Factory of DynamoDb Akka Stream operators.
 */
object DynamoDb {

  /**
   * Create a Flow that emits a response for every request to DynamoDB.
   *
   * @param parallelism maximum number of in-flight requests at any given time
   */
  def flow[In <: DynamoDbRequest, Out <: DynamoDbResponse](
      parallelism: Int
  )(implicit client: DynamoDbAsyncClient, operation: DynamoDbOp[In, Out]): Flow[In, Out, NotUsed] =
    Flow[In].mapAsync(parallelism)(operation.execute(_))

  /**
   * Create a `FlowWithContext` that emits a response for every request to DynamoDB.
   * A successful response is wrapped in [[scala.util.Success]] and a failed
   * response is wrapped in [[scala.util.Failure]].
   *
   * The context is merely passed through to the emitted element.
   *
   * @param parallelism maximum number of in-flight requests at any given time
   * @tparam Ctx context (or pass-through)
   */
  def flowWithContext[In <: DynamoDbRequest, Out <: DynamoDbResponse, Ctx](
      parallelism: Int
  )(implicit client: DynamoDbAsyncClient,
    operation: DynamoDbOp[In, Out]): FlowWithContext[In, Ctx, Try[Out], Ctx, NotUsed] =
    FlowWithContext.fromTuples(
      Flow[(In, Ctx)]
        .mapAsync(parallelism) {
          case (in, ctx) =>
            operation
              .execute(in)
              .map[(Try[Out], Ctx)](res => (Success(res), ctx))(ExecutionContexts.sameThreadExecutionContext)
              .recover { case t => (Failure(t), ctx) }(ExecutionContexts.sameThreadExecutionContext)
        }
    )

  /**
   * Create a Source that will emit potentially multiple responses for a given request.
   */
  def source[In <: DynamoDbRequest, Out <: DynamoDbResponse, Pub <: SdkPublisher[Out]](
      request: In
  )(implicit client: DynamoDbAsyncClient, operation: DynamoDbPaginatedOp[In, Out, Pub]): Source[Out, NotUsed] =
    Source.fromPublisher(operation.publisher(request))

  /**
   * Create a Future that will be completed with a response to a given request.
   */
  def single[In <: DynamoDbRequest, Out <: DynamoDbResponse](
      request: In
  )(implicit client: DynamoDbAsyncClient, operation: DynamoDbOp[In, Out], mat: Materializer): Future[Out] =
    Source.single(request).via(flow(1)).runWith(Sink.head)
}
