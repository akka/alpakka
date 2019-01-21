/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.impl.{Paginator, Setup}
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp, DynamoClientExt}
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

/**
 * Factory of DynamoDB Akka Stream operators.
 */
object DynamoDb {

  /**
   * Create a Flow that emits a response for every request to DynamoDB.
   */
  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] =
    Setup
      .flow { mat => _ =>
        DynamoClientExt(mat.system).dynamoClient.underlying.flow[Op]
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a Source that will emit a response for a given request.
   *
   * @param op request to send
   */
  def source(op: AwsPagedOp): Source[op.B, NotUsed] =
    Setup
      .source { mat => _ =>
        Paginator.source(DynamoClientExt(mat.system).dynamoClient.underlying.flow, op)
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a Source that will emit a response for a given request.
   *
   * @param op request to send
   */
  def source(op: AwsOp): Source[op.B, NotUsed] =
    Source.single(op).via(flow).map(_.asInstanceOf[op.B])

  /**
   * Create a Future that will be completed with a response to a given request.
   *
   * @param op request to send
   * @param mat materialized that will be used to run the stream
   */
  def single(op: AwsOp)(implicit mat: Materializer): Future[op.B] =
    source(op).runWith(Sink.head)
}
