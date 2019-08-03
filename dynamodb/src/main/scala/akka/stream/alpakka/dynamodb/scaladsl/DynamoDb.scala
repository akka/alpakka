/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.stream.{FlowShape, Materializer}
import akka.stream.alpakka.dynamodb.{DynamoDbOp, DynamoDbPaginatedOp}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source, ZipWith2}
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
   */
  def flow[In <: DynamoDbRequest, Out <: DynamoDbResponse](
      parallelism: Int
  )(implicit client: DynamoDbAsyncClient, operation: DynamoDbOp[In, Out]): Flow[In, Out, NotUsed] =
    Flow[In].mapAsync(parallelism)(operation.execute(_))

  /**
   * Create a Flow that emits a response for every request to DynamoDB.
   * A successful response is wrapped in [scala.util.success] and a failed
   * response is wrapped in [scala.util.Failure].
   */
  def tryFlow[In <: DynamoDbRequest, Out <: DynamoDbResponse, State](
      parallelism: Int
  )(implicit client: DynamoDbAsyncClient,
    operation: DynamoDbOp[In, Out]): Flow[(In, State), (Try[Out], State), NotUsed] =
    Flow
      .setup { (mat, _) =>
        implicit val ec = mat.system.dispatcher
        val operationFlow = Flow[In].mapAsync(parallelism)(
          // after 2.11 is dropped, replace this with operation.execute(_).transformWith(Future.successful)
          operation.execute(_).map(Success.apply).recover { case t => Failure(t) }
        )
        Flow.fromGraph {
          GraphDSL.create(operationFlow) { implicit b => flow =>
            import GraphDSL.Implicits._
            val broadcast = b.add(new Broadcast[(In, State)](outputPorts = 2, eagerCancel = true))
            val zip = b.add(new ZipWith2[Try[Out], State, (Try[Out], State)]((out, state) => (out, state)))

            broadcast.out(0).map(_._1) ~> flow ~> zip.in0
            broadcast.out(1).map(_._2) ~> zip.in1

            FlowShape(broadcast.in, zip.out)
          }
        }

      }
      .mapMaterializedValue(_ => NotUsed)

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
