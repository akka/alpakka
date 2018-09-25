/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl
import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.impl.Paginator
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp, DynamoClient}
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

/**
 * A factory of operations that use provided DynamoClient.
 */
object DynamoDbExternal {

  /**
   * Create a Flow that emits a response for every request.
   *
   * @param client DynamoClient instance that will be used to send the request
   */
  def flow[Op <: AwsOp](implicit client: DynamoClient): Flow[Op, Op#B, NotUsed] =
    client.underlying.flow[Op]

  /**
   * Create a Source that will emit a response for a given request.
   *
   * @param op request to send
   * @param client DynamoClient instance that will be used to send the request
   */
  def source(op: AwsPagedOp)(implicit client: DynamoClient): Source[op.B, NotUsed] =
    Paginator.source(client.underlying.flow, op)

  /**
   * Create a Source that will emit a response for a given request.
   *
   * @param op request to send
   * @param client DynamoClient instance that will be used to send the request
   */
  def source(op: AwsOp)(implicit client: DynamoClient): Source[op.B, NotUsed] =
    Source.single(op).via(client.underlying.flow).map(_.asInstanceOf[op.B])

  /**
   * Create a Future that will be completed with a response to a given request.
   *
   * @param op request to send
   * @param client DynamoClient instance that will be used to send the request
   */
  def single(op: AwsOp)(implicit client: DynamoClient): Future[op.B] = {
    implicit val mat: Materializer = client.materializer
    Source.single(op).via(client.underlying.flow).map(_.asInstanceOf[op.B]).runWith(Sink.head[op.B])
  }
}
