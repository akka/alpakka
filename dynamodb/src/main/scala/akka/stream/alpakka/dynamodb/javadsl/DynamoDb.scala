/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp}
import akka.stream.alpakka.dynamodb.scaladsl
import akka.stream.javadsl.{Flow, Sink, Source}
import com.amazonaws.services.dynamodbv2.model._

/**
 * Factory of DynamoDb Akka Stream operators.
 */
object DynamoDb {

  /**
   * Create a Flow that emits a response for every request.
   */
  def flowOp[Op <: AwsOp](op: Op): Flow[Op, Op#B, NotUsed] =
    scaladsl.DynamoDb.flowOp(op).asJava

  /**
   * Create a Flow that emits a response for every request.
   */
  @deprecated("User flowOp instead", "")
  def flow[Op <: AwsOp]: Flow[Op, Op#B, NotUsed] =
    scaladsl.DynamoDb.flow.asJava

  /**
   * Create a Source that will emit potentially multiple responses for a given request.
   */
  def source(op: AwsPagedOp): Source[op.B, NotUsed] =
    scaladsl.DynamoDb.source(op).asJava

  /**
   * Create a Source that will emit a response for a given request.
   */
  def source(op: AwsOp): Source[op.B, NotUsed] =
    scaladsl.DynamoDb.source(op).asJava

  /**
   * Create a CompletionStage that will be completed with a response to a given request.
   */
  def single[Op <: AwsOp](op: Op, mat: Materializer): CompletionStage[Op#B] =
    source(op).runWith(Sink.head(), mat)

  def batchGetItem(request: BatchGetItemRequest): Source[BatchGetItemResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def createTable(request: CreateTableRequest): Source[CreateTableResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def deleteItem(request: DeleteItemRequest): Source[DeleteItemResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def deleteTable(request: DeleteTableRequest): Source[DeleteTableResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def describeLimits(request: DescribeLimitsRequest): Source[DescribeLimitsResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def describeTable(request: DescribeTableRequest): Source[DescribeTableResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def describeTimeToLive(request: DescribeTimeToLiveRequest): Source[DescribeTimeToLiveResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def query(request: QueryRequest): Source[QueryResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def queryAll(request: QueryRequest): Source[QueryResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def scan(request: ScanRequest): Source[ScanResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def scanAll(request: ScanRequest): Source[ScanResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def updateItem(request: UpdateItemRequest): Source[UpdateItemResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def updateTable(request: UpdateTableRequest): Source[UpdateTableResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def putItem(request: PutItemRequest): Source[PutItemResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def batchWriteItem(request: BatchWriteItemRequest): Source[BatchWriteItemResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def getItem(request: GetItemRequest): Source[GetItemResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def listTables(request: ListTablesRequest): Source[ListTablesResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def transactGetItems(request: TransactGetItemsRequest): Source[TransactGetItemsResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def transactWriteItems(request: TransactWriteItemsRequest): Source[TransactWriteItemsResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava

  def updateTimeToLive(request: UpdateTimeToLiveRequest): Source[UpdateTimeToLiveResult, NotUsed] =
    scaladsl.DynamoDb.source(request).asJava
}
