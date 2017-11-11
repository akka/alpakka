/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.impl.{DynamoClientImpl, DynamoSettings}
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object DynamoClient {
  def apply(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer) =
    new DynamoClient(settings)
}

final class DynamoClient(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer) {
  private val client = new DynamoClientImpl(settings, DynamoImplicits.errorResponseHandler)

  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] = client.flow[Op]

  def source(op: AwsOp): Source[op.B, NotUsed] =
    Source.single(op).via(client.flow).map(_.asInstanceOf[op.B])

  def single(op: AwsOp): Future[op.B] =
    Source.single(op).via(client.flow).map(_.asInstanceOf[op.B]).runWith(Sink.head[op.B])
}
