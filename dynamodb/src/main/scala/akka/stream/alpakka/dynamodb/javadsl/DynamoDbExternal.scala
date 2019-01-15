/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp, DynamoClient}
import akka.stream.alpakka.dynamodb.scaladsl
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits._
import akka.stream.javadsl.{Flow, Source}
import com.amazonaws.services.dynamodbv2.model._

import scala.compat.java8.FutureConverters._

/**
 * A factory of operations that use provided DynamoClient.
 */
object DynamoDbExternal {

  /**
   * Create a Flow that emits a response for every request.
   *
   * @param client DynamoClient instance that will be used to send the request
   */
  def flow[Op <: AwsOp](client: DynamoClient): Flow[Op, Op#B, NotUsed] =
    scaladsl.DynamoDbExternal.flow(client).asJava

  private def source(op: AwsPagedOp, client: DynamoClient): Source[op.B, NotUsed] =
    scaladsl.DynamoDbExternal.source(op)(client).asJava

  private def single(op: AwsOp, client: DynamoClient): CompletionStage[op.B] =
    scaladsl.DynamoDbExternal.single(op)(client).toJava

  def batchGetItem(request: BatchGetItemRequest, client: DynamoClient): CompletionStage[BatchGetItemResult] =
    single(BatchGetItem(request), client)

  def createTable(request: CreateTableRequest, client: DynamoClient): CompletionStage[CreateTableResult] =
    single(CreateTable(request), client)

  def deleteItem(request: DeleteItemRequest, client: DynamoClient): CompletionStage[DeleteItemResult] =
    single(DeleteItem(request), client)

  def deleteTable(request: DeleteTableRequest, client: DynamoClient): CompletionStage[DeleteTableResult] =
    single(DeleteTable(request), client)

  def describeLimits(request: DescribeLimitsRequest, client: DynamoClient): CompletionStage[DescribeLimitsResult] =
    single(DescribeLimits(request), client)

  def describeTable(request: DescribeTableRequest, client: DynamoClient): CompletionStage[DescribeTableResult] =
    single(DescribeTable(request), client)

  def describeTimeToLive(request: DescribeTimeToLiveRequest,
                         client: DynamoClient): CompletionStage[DescribeTimeToLiveResult] =
    single(DescribeTimeToLive(request), client)

  def query(request: QueryRequest, client: DynamoClient): CompletionStage[QueryResult] =
    single(Query(request), client)

  def queryAll(request: QueryRequest, client: DynamoClient): Source[QueryResult, NotUsed] =
    source(Query(request), client)

  def scan(request: ScanRequest, client: DynamoClient): CompletionStage[ScanResult] =
    single(Scan(request), client)

  def scanAll(request: ScanRequest, client: DynamoClient): Source[ScanResult, NotUsed] =
    source(Scan(request), client)

  def updateItem(request: UpdateItemRequest, client: DynamoClient): CompletionStage[UpdateItemResult] =
    single(UpdateItem(request), client)

  def updateTable(request: UpdateTableRequest, client: DynamoClient): CompletionStage[UpdateTableResult] =
    single(UpdateTable(request), client)

  def putItem(request: PutItemRequest, client: DynamoClient): CompletionStage[PutItemResult] =
    single(PutItem(request), client)

  def batchWriteItem(request: BatchWriteItemRequest, client: DynamoClient): CompletionStage[BatchWriteItemResult] =
    single(BatchWriteItem(request), client)

  def getItem(request: GetItemRequest, client: DynamoClient): CompletionStage[GetItemResult] =
    single(GetItem(request), client)

  def listTables(request: ListTablesRequest, client: DynamoClient): CompletionStage[ListTablesResult] =
    single(ListTables(request), client)

  def transactGetItems(request: TransactGetItemsRequest,
                       client: DynamoClient): CompletionStage[TransactGetItemsResult] =
    single(TransactGetItems(request), client)

  def transactWriteItems(request: TransactWriteItemsRequest,
                         client: DynamoClient): CompletionStage[TransactWriteItemsResult] =
    single(TransactWriteItems(request), client)

  def updateTimeToLive(request: UpdateTimeToLiveRequest,
                       client: DynamoClient): CompletionStage[UpdateTimeToLiveResult] =
    single(UpdateTimeToLive(request), client)
}
