/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{javadsl, Materializer}
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp}
import akka.stream.alpakka.dynamodb.impl.{DynamoClientImpl, DynamoSettings, Paginator}
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.services.dynamodbv2.model._
import scala.compat.java8.FutureConverters._

import scala.concurrent.ExecutionContextExecutor

object DynamoClient {
  def create(settings: DynamoSettings, system: ActorSystem, materializer: Materializer) =
    new DynamoClient(settings)(system, materializer)
}

final class DynamoClient(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer) {

  private val client = new DynamoClientImpl(settings, DynamoImplicits.errorResponseHandler)
  private implicit val ec: ExecutionContextExecutor = system.dispatcher

  import DynamoImplicits._

  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] = client.flow.asJava

  private def single(op: AwsOp): CompletionStage[op.B] =
    Source.single(op).via(client.flow).runWith(Sink.head).map(_.asInstanceOf[op.B]).toJava

  private def source(op: AwsPagedOp): javadsl.Source[op.B, NotUsed] =
    Paginator.source(client.flow, op).asJava

  def batchGetItem(request: BatchGetItemRequest): CompletionStage[BatchGetItemResult] = single(BatchGetItem(request))

  def createTable(request: CreateTableRequest): CompletionStage[CreateTableResult] = single(CreateTable(request))

  def deleteItem(request: DeleteItemRequest): CompletionStage[DeleteItemResult] = single(DeleteItem(request))

  def deleteTable(request: DeleteTableRequest): CompletionStage[DeleteTableResult] = single(DeleteTable(request))

  def describeLimits(request: DescribeLimitsRequest): CompletionStage[DescribeLimitsResult] =
    single(DescribeLimits(request))

  def describeTable(request: DescribeTableRequest): CompletionStage[DescribeTableResult] =
    single(DescribeTable(request))

  def describeTimeToLive(request: DescribeTimeToLiveRequest): CompletionStage[DescribeTimeToLiveResult] =
    single(DescribeTimeToLive(request))

  def query(request: QueryRequest): CompletionStage[QueryResult] = single(Query(request))

  def queryAll(request: QueryRequest): javadsl.Source[QueryResult, NotUsed] = source(Query(request))

  def scan(request: ScanRequest): CompletionStage[ScanResult] = single(Scan(request))

  def scanAll(request: ScanRequest): javadsl.Source[ScanResult, NotUsed] = source(Scan(request))

  def updateItem(request: UpdateItemRequest): CompletionStage[UpdateItemResult] = single(UpdateItem(request))

  def updateTable(request: UpdateTableRequest): CompletionStage[UpdateTableResult] = single(UpdateTable(request))

  def putItem(request: PutItemRequest): CompletionStage[PutItemResult] = single(PutItem(request))

  def batchWriteItem(request: BatchWriteItemRequest): CompletionStage[BatchWriteItemResult] =
    single(BatchWriteItem(request))

  def getItem(request: GetItemRequest): CompletionStage[GetItemResult] = single(GetItem(request))

  def listTables(request: ListTablesRequest): CompletionStage[ListTablesResult] = single(ListTables(request))

  def updateTimeToLive(request: UpdateTimeToLiveRequest): CompletionStage[UpdateTimeToLiveResult] =
    single(UpdateTimeToLive(request))

}
