/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp, DynamoSettings}
import akka.stream.alpakka.dynamodb.impl.{DynamoClientImpl, Paginator}
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

trait DynamoClient {
  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed]
  def source(op: AwsPagedOp): Source[op.B, NotUsed]
  def source(op: AwsOp): Source[op.B, NotUsed]
  def single(op: AwsOp): Future[op.B]
}

object DynamoClient {
  def apply(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer): DynamoClient =
    new DynamoClient {
      private val client = new DynamoClientImpl(settings, DynamoImplicits.errorResponseHandler)

      def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] = client.flow[Op]

      def source(op: AwsPagedOp): Source[op.B, NotUsed] =
        Paginator.source(client.flow, op)

      def source(op: AwsOp): Source[op.B, NotUsed] =
        Source.single(op).via(client.flow).map(_.asInstanceOf[op.B])

      def single(op: AwsOp): Future[op.B] =
        Source.single(op).via(client.flow).map(_.asInstanceOf[op.B]).runWith(Sink.head[op.B])
    }
}
