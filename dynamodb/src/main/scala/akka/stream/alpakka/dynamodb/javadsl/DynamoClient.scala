/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb.javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.impl.{DynamoClientImpl, DynamoSettings, Streams}
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits
import akka.stream.alpakka.dynamodb.scaladsl.{DynamoClient => ScalaDynamoClient}
import akka.stream.javadsl.{Flow, Source}
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.{Source => ScalaSource}
import com.amazonaws.services.dynamodbv2.model._

import scala.concurrent.Future

object DynamoClient {
  def create(settings: DynamoSettings, system: ActorSystem, materializer: Materializer) =
    new DynamoClient(settings)(system, materializer)
}

final class DynamoClient(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer) {

  private val client = new DynamoClientImpl(settings, DynamoImplicits.errorResponseHandler)
  private implicit val ec = system.dispatcher

  import DynamoImplicits._

  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] = client.flow.asJava

  def records(describeTableRequest: DescribeTableRequest): Source[Record, NotUsed] =
    Streams.records(describeTableRequest)(ScalaDynamoClient(client), ec).asJava

  private def single(op: AwsOp): Future[op.B] =
    ScalaSource.single(op).via(client.flow).runWith(Sink.head).map(_.asInstanceOf[op.B])

  def batchGetItem(request: BatchGetItemRequest) = single(BatchGetItem(request))

  def createTable(request: CreateTableRequest) = single(CreateTable(request))

  def deleteItem(request: DeleteItemRequest) = single(DeleteItem(request))

  def deleteTable(request: DeleteTableRequest) = single(DeleteTable(request))

  def describeLimits(request: DescribeLimitsRequest) = single(DescribeLimits(request))

  def describeTable(request: DescribeTableRequest) = single(DescribeTable(request))

  def query(request: QueryRequest) = single(Query(request))

  def scan(request: ScanRequest) = single(Scan(request))

  def updateItem(request: UpdateItemRequest) = single(UpdateItem(request))

  def updateTable(request: UpdateTableRequest) = single(UpdateTable(request))

  def putItem(request: PutItemRequest) = single(PutItem(request))

  def batchWriteItem(request: BatchWriteItemRequest) = single(BatchWriteItem(request))

  def getItem(request: GetItemRequest) = single(GetItem(request))

  def listTables(request: ListTablesRequest) = single(ListTables(request))

}
