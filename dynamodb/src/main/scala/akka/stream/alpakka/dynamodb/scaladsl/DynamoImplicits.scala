/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb.scaladsl

import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.impl.DynamoProtocol
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.transform.Marshaller
import com.amazonaws.{AmazonWebServiceResponse, Request}

object DynamoImplicits extends DynamoProtocol {

  implicit class BatchGetItem(val request: BatchGetItemRequest) extends AwsOp {
    override type A = BatchGetItemRequest
    override type B = BatchGetItemResult
    override val handler = batchGetItemU
    override val marshaller = batchGetItemM
  }

  implicit class CreateTable(val request: CreateTableRequest) extends AwsOp {
    override type A = CreateTableRequest
    override type B = CreateTableResult
    override val handler = createTableU
    override val marshaller = createTableM
  }

  implicit class DeleteItem(val request: DeleteItemRequest) extends AwsOp {
    override type A = DeleteItemRequest
    override type B = DeleteItemResult
    override val handler = deleteItemU
    override val marshaller = deleteItemM
  }

  implicit class DeleteTable(val request: DeleteTableRequest) extends AwsOp {
    override type A = DeleteTableRequest
    override type B = DeleteTableResult
    override val handler = deleteTableU
    override val marshaller = deleteTableM
  }

  implicit class DescribeLimits(val request: DescribeLimitsRequest) extends AwsOp {
    override type A = DescribeLimitsRequest
    override type B = DescribeLimitsResult
    override val handler = describeLimitsU
    override val marshaller = describeLimitsM
  }

  implicit class DescribeTable(val request: DescribeTableRequest) extends AwsOp {
    override type A = DescribeTableRequest
    override type B = DescribeTableResult
    override val handler = describeTableU
    override val marshaller = describeTableM
  }

  implicit class Query(val request: QueryRequest) extends AwsOp {
    override type A = QueryRequest
    override type B = QueryResult
    override val handler = queryU
    override val marshaller = queryM
  }

  implicit class Scan(val request: ScanRequest) extends AwsOp {
    override type A = ScanRequest
    override type B = ScanResult
    override val handler = scanU
    override val marshaller = scanM
  }

  implicit class UpdateItem(val request: UpdateItemRequest) extends AwsOp {
    override type A = UpdateItemRequest
    override type B = UpdateItemResult
    override val handler = updateItemU
    override val marshaller = updateItemM
  }

  implicit class UpdateTable(val request: UpdateTableRequest) extends AwsOp {
    override type A = UpdateTableRequest
    override type B = UpdateTableResult
    override val handler = updateTableU
    override val marshaller = updateTableM
  }

  implicit class PutItem(val request: PutItemRequest) extends AwsOp {
    override type A = PutItemRequest
    override type B = PutItemResult
    override val handler = putItemU
    override val marshaller = putItemM
  }

  implicit class BatchWriteItem(val request: BatchWriteItemRequest) extends AwsOp {
    override type A = BatchWriteItemRequest
    override type B = BatchWriteItemResult
    override val handler = batchWriteItemU
    override val marshaller = batchWriteItemM
  }

  implicit class GetItem(val request: GetItemRequest) extends AwsOp {
    override type A = GetItemRequest
    override type B = GetItemResult
    override val handler = getItemU
    override val marshaller = getItemM
  }

  implicit class ListTables(val request: ListTablesRequest) extends AwsOp {
    override type A = ListTablesRequest
    override type B = ListTablesResult
    override val handler = listTablesU
    override val marshaller = listTablesM
  }

}
