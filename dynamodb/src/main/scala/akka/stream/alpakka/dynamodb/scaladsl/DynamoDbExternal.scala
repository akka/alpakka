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

object DynamoDbExternal {
  def flow[Op <: AwsOp](implicit client: DynamoClient): Flow[Op, Op#B, NotUsed] =
    client.underlying.flow[Op]

  def source(op: AwsPagedOp)(implicit client: DynamoClient): Source[op.B, NotUsed] =
    Paginator.source(client.underlying.flow, op)

  def source(op: AwsOp)(implicit client: DynamoClient): Source[op.B, NotUsed] =
    Source.single(op).via(client.underlying.flow).map(_.asInstanceOf[op.B])

  def single(op: AwsOp)(implicit client: DynamoClient): Future[op.B] = {
    implicit val mat: Materializer = client.materializer
    Source.single(op).via(client.underlying.flow).map(_.asInstanceOf[op.B]).runWith(Sink.head[op.B])
  }
}
