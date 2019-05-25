/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import java.util.concurrent.CompletableFuture

import org.reactivestreams.Publisher
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.paginators.{ListTablesPublisher, QueryPublisher, ScanPublisher}

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

sealed class AwsOp[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    sdkExecute: DynamoDbAsyncClient => In => CompletableFuture[Out]
) {
  def execute(request: In)(implicit client: DynamoDbAsyncClient): Future[Out] = sdkExecute(client)(request).toScala
}

sealed class AwsPaginatedOp[In <: DynamoDbRequest, Out <: DynamoDbResponse, Pub <: SdkPublisher[Out]](
    sdkExecute: DynamoDbAsyncClient => In => CompletableFuture[Out],
    sdkPublisher: DynamoDbAsyncClient => In => Pub
) extends AwsOp[In, Out](sdkExecute) {
  def publisher(request: In)(implicit client: DynamoDbAsyncClient): Publisher[Out] = sdkPublisher(client)(request)
}

object AwsOp {

  // format: off
  implicit val BatchGetItem :AwsOp[BatchGetItemRequest, BatchGetItemResponse] = new AwsOp[BatchGetItemRequest, BatchGetItemResponse](_.batchGetItem)
  implicit val BatchWriteItem :AwsOp[BatchWriteItemRequest, BatchWriteItemResponse] = new AwsOp[BatchWriteItemRequest, BatchWriteItemResponse](_.batchWriteItem)
  implicit val CreateTable :AwsOp[CreateTableRequest, CreateTableResponse] = new AwsOp[CreateTableRequest, CreateTableResponse](_.createTable)
  implicit val DeleteItem :AwsOp[DeleteItemRequest, DeleteItemResponse] = new AwsOp[DeleteItemRequest, DeleteItemResponse](_.deleteItem)
  implicit val DeleteTable :AwsOp[DeleteTableRequest, DeleteTableResponse] = new AwsOp[DeleteTableRequest, DeleteTableResponse](_.deleteTable)
  implicit val DescribeLimits :AwsOp[DescribeLimitsRequest, DescribeLimitsResponse] = new AwsOp[DescribeLimitsRequest, DescribeLimitsResponse](_.describeLimits)
  implicit val DescribeTable :AwsOp[DescribeTableRequest, DescribeTableResponse] = new AwsOp[DescribeTableRequest, DescribeTableResponse](_.describeTable)
  implicit val DescribeTimeToLive:AwsOp[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse] = new AwsOp[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse](_.describeTimeToLive)
  implicit val GetItem :AwsOp[GetItemRequest, GetItemResponse] = new AwsOp[GetItemRequest, GetItemResponse](_.getItem)
  implicit val ListTables :AwsPaginatedOp[ListTablesRequest, ListTablesResponse, ListTablesPublisher] = new AwsPaginatedOp[ListTablesRequest, ListTablesResponse, ListTablesPublisher](_.listTables, _.listTablesPaginator)
  implicit val PutItem :AwsOp[PutItemRequest, PutItemResponse] = new AwsOp[PutItemRequest, PutItemResponse](_.putItem)
  implicit val Query :AwsPaginatedOp[QueryRequest, QueryResponse, QueryPublisher] = new AwsPaginatedOp[QueryRequest, QueryResponse, QueryPublisher](_.query, _.queryPaginator)
  implicit val Scan :AwsPaginatedOp[ScanRequest, ScanResponse, ScanPublisher] = new AwsPaginatedOp[ScanRequest, ScanResponse, ScanPublisher](_.scan, _.scanPaginator)
  implicit val TransactGetItems :AwsOp[TransactGetItemsRequest, TransactGetItemsResponse] = new AwsOp[TransactGetItemsRequest, TransactGetItemsResponse](_.transactGetItems)
  implicit val TransactWriteItems:AwsOp[TransactWriteItemsRequest, TransactWriteItemsResponse] = new AwsOp[TransactWriteItemsRequest, TransactWriteItemsResponse](_.transactWriteItems)
  implicit val UpdateItem :AwsOp[UpdateItemRequest, UpdateItemResponse] = new AwsOp[UpdateItemRequest, UpdateItemResponse](_.updateItem)
  implicit val UpdateTable :AwsOp[UpdateTableRequest, UpdateTableResponse] = new AwsOp[UpdateTableRequest, UpdateTableResponse](_.updateTable)
  implicit val UpdateTimeToLive :AwsOp[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse] = new AwsOp[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse](_.updateTimeToLive)
  // format: on
}
