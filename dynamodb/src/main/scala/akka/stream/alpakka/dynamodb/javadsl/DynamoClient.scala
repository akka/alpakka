/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{javadsl, Materializer}
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp}
import akka.stream.alpakka.dynamodb.impl.{DynamoClientImpl, DynamoSettings, Paginator}
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.services.dynamodbv2.model._

import scala.concurrent.{ExecutionContextExecutor, Future}

object DynamoClient {
  def create(settings: DynamoSettings, system: ActorSystem, materializer: Materializer) =
    new DynamoClient(settings)(system, materializer)
}

final class DynamoClient(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer) {

  private val client = new DynamoClientImpl(settings, DynamoImplicits.errorResponseHandler)
  private implicit val ec: ExecutionContextExecutor = system.dispatcher

  import DynamoImplicits._

  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] = client.flow.asJava

  private def single(op: AwsOp): Future[op.B] =
    Source.single(op).via(client.flow).runWith(Sink.head).map(_.asInstanceOf[op.B])

  private def source(op: AwsPagedOp): javadsl.Source[op.B, NotUsed] =
    Paginator.source(client.flow, op).asJava

  def batchGetItem(request: BatchGetItemRequest): Future[BatchGetItemResult] = single(BatchGetItem(request))

  def createTable(request: CreateTableRequest): Future[CreateTableResult] = single(CreateTable(request))

  def deleteItem(request: DeleteItemRequest): Future[DeleteItemResult] = single(DeleteItem(request))

  def deleteTable(request: DeleteTableRequest): Future[DeleteTableResult] = single(DeleteTable(request))

  def describeLimits(request: DescribeLimitsRequest): Future[DescribeLimitsResult] = single(DescribeLimits(request))

  def describeTable(request: DescribeTableRequest): Future[DescribeTableResult] = single(DescribeTable(request))

  def describeTimeToLive(request: DescribeTimeToLiveRequest): Future[DescribeTimeToLiveResult] =
    single(DescribeTimeToLive(request))

  def query(request: QueryRequest): Future[QueryResult] = single(Query(request))

  def queryAll(request: QueryRequest): javadsl.Source[QueryResult, NotUsed] = source(Query(request))

  def scan(request: ScanRequest): Future[ScanResult] = single(Scan(request))

  def scanAll(request: ScanRequest): javadsl.Source[ScanResult, NotUsed] = source(Scan(request))

  def updateItem(request: UpdateItemRequest): Future[UpdateItemResult] = single(UpdateItem(request))

  def updateTable(request: UpdateTableRequest): Future[UpdateTableResult] = single(UpdateTable(request))

  def putItem(request: PutItemRequest): Future[PutItemResult] = single(PutItem(request))

  def batchWriteItem(request: BatchWriteItemRequest): Future[BatchWriteItemResult] = single(BatchWriteItem(request))

  def getItem(request: GetItemRequest): Future[GetItemResult] = single(GetItem(request))

  def listTables(request: ListTablesRequest): Future[ListTablesResult] = single(ListTables(request))

  def updateTimeToLive(request: UpdateTimeToLiveRequest): Future[UpdateTimeToLiveResult] =
    single(UpdateTimeToLive(request))

}
