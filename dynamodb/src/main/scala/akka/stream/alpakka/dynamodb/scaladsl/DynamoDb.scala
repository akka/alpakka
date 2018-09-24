/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp, DynamoClientExt}
import akka.stream.scaladsl.{Flow, Source}

import scala.concurrent.Future

/**
 * A factory of operations that use managed DynamoClient.
 */
object DynamoDb {

  /**
   * Create a Flow that emits a response for every request.
   *
   * @param sys actor system that will be used to resolved managed DynamoClient
   */
  def flow[Op <: AwsOp](implicit sys: ActorSystem): Flow[Op, Op#B, NotUsed] =
    DynamoDbExternal.flow(DynamoClientExt(sys).dynamoClient)

  /**
   * Create a Source that will emit a response for a given request.
   *
   * @param op request to send
   * @param sys actor system that will be used to resolved managed DynamoClient
   */
  def source(op: AwsPagedOp)(implicit sys: ActorSystem): Source[op.B, NotUsed] =
    DynamoDbExternal.source(op)(DynamoClientExt(sys).dynamoClient)

  /**
   * Create a Source that will emit a response for a given request.
   *
   * @param op request to send
   * @param sys actor system that will be used to resolved managed DynamoClient
   */
  def source(op: AwsOp)(implicit sys: ActorSystem): Source[op.B, NotUsed] =
    DynamoDbExternal.source(op)(DynamoClientExt(sys).dynamoClient)

  /**
   * Create a Future that will be completed with a response to a given request.
   *
   * @param op request to send
   * @param sys actor system that will be used to resolved managed DynamoClient
   */
  def single(op: AwsOp)(implicit sys: ActorSystem): Future[op.B] =
    DynamoDbExternal.single(op)(DynamoClientExt(sys).dynamoClient)
}
