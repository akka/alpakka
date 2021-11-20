/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import java.util.concurrent.CompletableFuture

import org.reactivestreams.Publisher
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.paginators.{
  BatchGetItemPublisher,
  ListTablesPublisher,
  QueryPublisher,
  ScanPublisher
}

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

/**
 * Representation on an AWS dynamodb sdk operation.
 *
 * See https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/basics-async.html
 * @param sdkExecute function to be executed on the AWS client
 * @tparam In dynamodb request type
 * @tparam Out dynamodb response type
 */
sealed class DynamoDbOp[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    sdkExecute: DynamoDbAsyncClient => In => CompletableFuture[Out]
) {
  def execute(request: In)(implicit client: DynamoDbAsyncClient): Future[Out] = sdkExecute(client)(request).toScala
}

/**
 * Representation on an AWS dynamodb sdk paginated operation
 *
 * See https://docs.aws.amazon.com/en_pv/sdk-for-java/v2/developer-guide/examples-pagination.html
 * @param sdkExecute function to be executed on the AWS client
 * @param sdkPublisher publisher to be called on the AWS client
 * @tparam In dynamodb request type
 * @tparam Out dynamodb response type
 */
sealed class DynamoDbPaginatedOp[In <: DynamoDbRequest, Out <: DynamoDbResponse, Pub <: SdkPublisher[Out]](
    sdkExecute: DynamoDbAsyncClient => In => CompletableFuture[Out],
    sdkPublisher: DynamoDbAsyncClient => In => Pub
) extends DynamoDbOp[In, Out](sdkExecute) {
  def publisher(request: In)(implicit client: DynamoDbAsyncClient): Publisher[Out] = sdkPublisher(client)(request)
}

object DynamoDbOp {

  // format: off
  // lower case names on purpose for nice Java API
  implicit val batchGetItem :DynamoDbPaginatedOp[BatchGetItemRequest, BatchGetItemResponse, BatchGetItemPublisher] = new DynamoDbPaginatedOp[BatchGetItemRequest, BatchGetItemResponse, BatchGetItemPublisher](_.batchGetItem, _.batchGetItemPaginator)
  implicit val batchWriteItem :DynamoDbOp[BatchWriteItemRequest, BatchWriteItemResponse] = new DynamoDbOp[BatchWriteItemRequest, BatchWriteItemResponse](_.batchWriteItem)
  implicit val createTable :DynamoDbOp[CreateTableRequest, CreateTableResponse] = new DynamoDbOp[CreateTableRequest, CreateTableResponse](_.createTable)
  implicit val deleteItem :DynamoDbOp[DeleteItemRequest, DeleteItemResponse] = new DynamoDbOp[DeleteItemRequest, DeleteItemResponse](_.deleteItem)
  implicit val deleteTable :DynamoDbOp[DeleteTableRequest, DeleteTableResponse] = new DynamoDbOp[DeleteTableRequest, DeleteTableResponse](_.deleteTable)
  implicit val describeLimits :DynamoDbOp[DescribeLimitsRequest, DescribeLimitsResponse] = new DynamoDbOp[DescribeLimitsRequest, DescribeLimitsResponse](_.describeLimits)
  implicit val describeTable :DynamoDbOp[DescribeTableRequest, DescribeTableResponse] = new DynamoDbOp[DescribeTableRequest, DescribeTableResponse](_.describeTable)
  implicit val describeTimeToLive:DynamoDbOp[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse] = new DynamoDbOp[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse](_.describeTimeToLive)
  implicit val getItem :DynamoDbOp[GetItemRequest, GetItemResponse] = new DynamoDbOp[GetItemRequest, GetItemResponse](_.getItem)
  implicit val listTables :DynamoDbPaginatedOp[ListTablesRequest, ListTablesResponse, ListTablesPublisher] = new DynamoDbPaginatedOp[ListTablesRequest, ListTablesResponse, ListTablesPublisher](_.listTables, _.listTablesPaginator)
  implicit val putItem :DynamoDbOp[PutItemRequest, PutItemResponse] = new DynamoDbOp[PutItemRequest, PutItemResponse](_.putItem)
  implicit val query :DynamoDbPaginatedOp[QueryRequest, QueryResponse, QueryPublisher] = new DynamoDbPaginatedOp[QueryRequest, QueryResponse, QueryPublisher](_.query, _.queryPaginator)
  implicit val scan :DynamoDbPaginatedOp[ScanRequest, ScanResponse, ScanPublisher] = new DynamoDbPaginatedOp[ScanRequest, ScanResponse, ScanPublisher](_.scan, _.scanPaginator)
  implicit val transactGetItems :DynamoDbOp[TransactGetItemsRequest, TransactGetItemsResponse] = new DynamoDbOp[TransactGetItemsRequest, TransactGetItemsResponse](_.transactGetItems)
  implicit val transactWriteItems:DynamoDbOp[TransactWriteItemsRequest, TransactWriteItemsResponse] = new DynamoDbOp[TransactWriteItemsRequest, TransactWriteItemsResponse](_.transactWriteItems)
  implicit val updateItem :DynamoDbOp[UpdateItemRequest, UpdateItemResponse] = new DynamoDbOp[UpdateItemRequest, UpdateItemResponse](_.updateItem)
  implicit val updateTable :DynamoDbOp[UpdateTableRequest, UpdateTableResponse] = new DynamoDbOp[UpdateTableRequest, UpdateTableResponse](_.updateTable)
  implicit val updateTimeToLive :DynamoDbOp[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse] = new DynamoDbOp[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse](_.updateTimeToLive)
  // format: on
}
