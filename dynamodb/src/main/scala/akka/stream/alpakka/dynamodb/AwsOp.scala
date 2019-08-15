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

/**
 * Representation on an AWS dynamodb sdk operation
 * @param sdkExecute function to be executed on the AWS client
 * @tparam In dynamodb request type
 * @tparam Out dynamodb response type
 */
sealed class AwsOp[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    sdkExecute: DynamoDbAsyncClient => In => CompletableFuture[Out]
) {
  def execute(request: In)(implicit client: DynamoDbAsyncClient): Future[Out] = sdkExecute(client)(request).toScala
}

/**
 * Representation on an AWS dynamodb sdk paginated operation
 * @param sdkExecute function to be executed on the AWS client
 * @param sdkPublisher publisher to be called on the AWS client
 * @tparam In dynamodb request type
 * @tparam Out dynamodb response type
 */
sealed class AwsPaginatedOp[In <: DynamoDbRequest, Out <: DynamoDbResponse, Pub <: SdkPublisher[Out]](
    sdkExecute: DynamoDbAsyncClient => In => CompletableFuture[Out],
    sdkPublisher: DynamoDbAsyncClient => In => Pub
) extends AwsOp[In, Out](sdkExecute) {
  def publisher(request: In)(implicit client: DynamoDbAsyncClient): Publisher[Out] = sdkPublisher(client)(request)
}

object AwsOp {

  // format: off
  // lower case names on purpose for nice Java API
  implicit val batchGetItem :AwsOp[BatchGetItemRequest, BatchGetItemResponse] = new AwsOp[BatchGetItemRequest, BatchGetItemResponse](_.batchGetItem)
  implicit val batchWriteItem :AwsOp[BatchWriteItemRequest, BatchWriteItemResponse] = new AwsOp[BatchWriteItemRequest, BatchWriteItemResponse](_.batchWriteItem)
  implicit val createTable :AwsOp[CreateTableRequest, CreateTableResponse] = new AwsOp[CreateTableRequest, CreateTableResponse](_.createTable)
  implicit val deleteItem :AwsOp[DeleteItemRequest, DeleteItemResponse] = new AwsOp[DeleteItemRequest, DeleteItemResponse](_.deleteItem)
  implicit val deleteTable :AwsOp[DeleteTableRequest, DeleteTableResponse] = new AwsOp[DeleteTableRequest, DeleteTableResponse](_.deleteTable)
  implicit val describeLimits :AwsOp[DescribeLimitsRequest, DescribeLimitsResponse] = new AwsOp[DescribeLimitsRequest, DescribeLimitsResponse](_.describeLimits)
  implicit val describeTable :AwsOp[DescribeTableRequest, DescribeTableResponse] = new AwsOp[DescribeTableRequest, DescribeTableResponse](_.describeTable)
  implicit val describeTimeToLive:AwsOp[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse] = new AwsOp[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse](_.describeTimeToLive)
  implicit val getItem :AwsOp[GetItemRequest, GetItemResponse] = new AwsOp[GetItemRequest, GetItemResponse](_.getItem)
  implicit val listTables :AwsPaginatedOp[ListTablesRequest, ListTablesResponse, ListTablesPublisher] = new AwsPaginatedOp[ListTablesRequest, ListTablesResponse, ListTablesPublisher](_.listTables, _.listTablesPaginator)
  implicit val putItem :AwsOp[PutItemRequest, PutItemResponse] = new AwsOp[PutItemRequest, PutItemResponse](_.putItem)
  implicit val query :AwsPaginatedOp[QueryRequest, QueryResponse, QueryPublisher] = new AwsPaginatedOp[QueryRequest, QueryResponse, QueryPublisher](_.query, _.queryPaginator)
  implicit val scan :AwsPaginatedOp[ScanRequest, ScanResponse, ScanPublisher] = new AwsPaginatedOp[ScanRequest, ScanResponse, ScanPublisher](_.scan, _.scanPaginator)
  implicit val transactGetItems :AwsOp[TransactGetItemsRequest, TransactGetItemsResponse] = new AwsOp[TransactGetItemsRequest, TransactGetItemsResponse](_.transactGetItems)
  implicit val transactWriteItems:AwsOp[TransactWriteItemsRequest, TransactWriteItemsResponse] = new AwsOp[TransactWriteItemsRequest, TransactWriteItemsResponse](_.transactWriteItems)
  implicit val updateItem :AwsOp[UpdateItemRequest, UpdateItemResponse] = new AwsOp[UpdateItemRequest, UpdateItemResponse](_.updateItem)
  implicit val updateTable :AwsOp[UpdateTableRequest, UpdateTableResponse] = new AwsOp[UpdateTableRequest, UpdateTableResponse](_.updateTable)
  implicit val updateTimeToLive :AwsOp[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse] = new AwsOp[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse](_.updateTimeToLive)
  // format: on
}
