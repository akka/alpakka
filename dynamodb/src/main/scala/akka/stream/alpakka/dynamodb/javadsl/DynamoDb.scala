/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb._
import akka.stream.javadsl.{Flow, Source}
import com.amazonaws.services.dynamodbv2.model._

object DynamoDb {
  def flow[Op <: AwsOp](sys: ActorSystem): Flow[Op, Op#B, NotUsed] =
    DynamoDbExternal.flow(DynamoClientExt(sys).dynamoClient)

  def batchGetItem(request: BatchGetItemRequest, sys: ActorSystem): CompletionStage[BatchGetItemResult] =
    DynamoDbExternal.batchGetItem(request, DynamoClientExt(sys).dynamoClient)

  def createTable(request: CreateTableRequest, sys: ActorSystem): CompletionStage[CreateTableResult] =
    DynamoDbExternal.createTable(request, DynamoClientExt(sys).dynamoClient)

  def deleteItem(request: DeleteItemRequest, sys: ActorSystem): CompletionStage[DeleteItemResult] =
    DynamoDbExternal.deleteItem(request, DynamoClientExt(sys).dynamoClient)

  def deleteTable(request: DeleteTableRequest, sys: ActorSystem): CompletionStage[DeleteTableResult] =
    DynamoDbExternal.deleteTable(request, DynamoClientExt(sys).dynamoClient)

  def describeLimits(request: DescribeLimitsRequest, sys: ActorSystem): CompletionStage[DescribeLimitsResult] =
    DynamoDbExternal.describeLimits(request, DynamoClientExt(sys).dynamoClient)

  def describeTable(request: DescribeTableRequest, sys: ActorSystem): CompletionStage[DescribeTableResult] =
    DynamoDbExternal.describeTable(request, DynamoClientExt(sys).dynamoClient)

  def describeTimeToLive(request: DescribeTimeToLiveRequest,
                         sys: ActorSystem): CompletionStage[DescribeTimeToLiveResult] =
    DynamoDbExternal.describeTimeToLive(request, DynamoClientExt(sys).dynamoClient)

  def query(request: QueryRequest, sys: ActorSystem): CompletionStage[QueryResult] =
    DynamoDbExternal.query(request, DynamoClientExt(sys).dynamoClient)

  def queryAll(request: QueryRequest, sys: ActorSystem): Source[QueryResult, NotUsed] =
    DynamoDbExternal.queryAll(request, DynamoClientExt(sys).dynamoClient)

  def scan(request: ScanRequest, sys: ActorSystem): CompletionStage[ScanResult] =
    DynamoDbExternal.scan(request, DynamoClientExt(sys).dynamoClient)

  def scanAll(request: ScanRequest, sys: ActorSystem): Source[ScanResult, NotUsed] =
    DynamoDbExternal.scanAll(request, DynamoClientExt(sys).dynamoClient)

  def updateItem(request: UpdateItemRequest, sys: ActorSystem): CompletionStage[UpdateItemResult] =
    DynamoDbExternal.updateItem(request, DynamoClientExt(sys).dynamoClient)

  def updateTable(request: UpdateTableRequest, sys: ActorSystem): CompletionStage[UpdateTableResult] =
    DynamoDbExternal.updateTable(request, DynamoClientExt(sys).dynamoClient)

  def putItem(request: PutItemRequest, sys: ActorSystem): CompletionStage[PutItemResult] =
    DynamoDbExternal.putItem(request, DynamoClientExt(sys).dynamoClient)

  def batchWriteItem(request: BatchWriteItemRequest, sys: ActorSystem): CompletionStage[BatchWriteItemResult] =
    DynamoDbExternal.batchWriteItem(request, DynamoClientExt(sys).dynamoClient)

  def getItem(request: GetItemRequest, sys: ActorSystem): CompletionStage[GetItemResult] =
    DynamoDbExternal.getItem(request, DynamoClientExt(sys).dynamoClient)

  def listTables(request: ListTablesRequest, sys: ActorSystem): CompletionStage[ListTablesResult] =
    DynamoDbExternal.listTables(request, DynamoClientExt(sys).dynamoClient)

  def updateTimeToLive(request: UpdateTimeToLiveRequest, sys: ActorSystem): CompletionStage[UpdateTimeToLiveResult] =
    DynamoDbExternal.updateTimeToLive(request, DynamoClientExt(sys).dynamoClient)
}
