/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.stream.{ActorMaterializer, Attributes, Materializer}
import akka.stream.alpakka.dynamodb.impl.{Paginator, Setup}
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp, DynamoAttributes, DynamoClientExt}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.amazonaws.{AmazonWebServiceResult, ResponseMetadata}

import scala.concurrent.Future

/**
 * Factory of DynamoDb Akka Stream operators.
 */
object DynamoDb {

  /**
   * Create a Flow that emits a response for every request to DynamoDB.
   */
  @deprecated("Use flowOp instead", "")
  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] =
    Setup
      .flow(clientFlow[Op])
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a Flow that emits a response for every request to DynamoDB.
   */
  def flowOp[Op <: AwsOp](op: Op): Flow[Op, Op#B, NotUsed] =
    Setup
      .flow(clientFlowOp[Op](op))
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a Source that will emit potentially multiple responses for a given request.
   */
  def source(op: AwsPagedOp): Source[op.B, NotUsed] =
    Setup
      .source { mat => attr =>
        Paginator.source(
          clientFlowOp(op)(mat)(attr).asInstanceOf[Flow[AwsOp, AmazonWebServiceResult[ResponseMetadata], NotUsed]],
          op
        )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a Source that will emit a response for a given request.
   */
  def source(op: AwsOp): Source[op.B, NotUsed] =
    Source.single(op).via(flowOp(op)).map(_.asInstanceOf[op.B])

  /**
   * Create a Future that will be completed with a response to a given request.
   */
  def single(op: AwsOp)(implicit mat: Materializer): Future[op.B] =
    source(op).runWith(Sink.head)

  private def clientFlowOp[Op <: AwsOp](op: Op)(mat: ActorMaterializer)(attr: Attributes) =
    attr
      .get[DynamoAttributes.Client]
      .map(_.client)
      .getOrElse(DynamoClientExt(mat.system).dynamoClient)
      .underlying
      .flowOp[Op](op)

  private def clientFlow[Op <: AwsOp](mat: ActorMaterializer)(attr: Attributes) =
    attr
      .get[DynamoAttributes.Client]
      .map(_.client)
      .getOrElse(DynamoClientExt(mat.system).dynamoClient)
      .underlying
      .flow[Op]
}
