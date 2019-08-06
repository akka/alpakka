/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.annotation.InternalApi
import com.amazonaws.AmazonServiceException
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.protocol.json._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model.transform._

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] trait DynamoProtocol {

  val meta = new JsonOperationMetadata().withPayloadJson(true)

  val protocol: SdkJsonProtocolFactory = new SdkJsonProtocolFactory(
    new JsonClientMetadata()
      .addAllErrorMetadata(
        new JsonErrorShapeMetadata()
          .withErrorCode("ItemCollectionSizeLimitExceededException")
          .withModeledClass(classOf[ItemCollectionSizeLimitExceededException]),
        new JsonErrorShapeMetadata()
          .withErrorCode("ResourceInUseException")
          .withModeledClass(classOf[ResourceInUseException]),
        new JsonErrorShapeMetadata()
          .withErrorCode("ResourceNotFoundException")
          .withModeledClass(classOf[ResourceNotFoundException]),
        new JsonErrorShapeMetadata()
          .withErrorCode("ProvisionedThroughputExceededException")
          .withModeledClass(classOf[ProvisionedThroughputExceededException]),
        new JsonErrorShapeMetadata()
          .withErrorCode("ConditionalCheckFailedException")
          .withModeledClass(classOf[ConditionalCheckFailedException]),
        new JsonErrorShapeMetadata()
          .withErrorCode("InternalServerError")
          .withModeledClass(classOf[InternalServerErrorException]),
        new JsonErrorShapeMetadata()
          .withErrorCode("LimitExceededException")
          .withModeledClass(classOf[LimitExceededException]),
        new JsonErrorShapeMetadata()
          .withErrorCode("TransactionCanceledException")
          .withModeledClass(classOf[TransactionCanceledException])
      )
      .withBaseServiceExceptionClass(classOf[com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException])
  )

  val errorResponseHandler: HttpResponseHandler[AmazonServiceException] =
    protocol.createErrorResponseHandler(new JsonErrorResponseMetadata())

  protected val batchGetItemM = new BatchGetItemRequestProtocolMarshaller(protocol)
  protected val batchWriteItemM = new BatchWriteItemRequestProtocolMarshaller(protocol)
  protected val createTableM = new CreateTableRequestProtocolMarshaller(protocol)
  protected val deleteItemM = new DeleteItemRequestProtocolMarshaller(protocol)
  protected val deleteTableM = new DeleteTableRequestProtocolMarshaller(protocol)
  protected val describeLimitsM = new DescribeLimitsRequestProtocolMarshaller(protocol)
  protected val describeTableM = new DescribeTableRequestProtocolMarshaller(protocol)
  protected val describeTimeToLiveM = new DescribeTimeToLiveRequestProtocolMarshaller(protocol)
  protected val getItemM = new GetItemRequestProtocolMarshaller(protocol)
  protected val listTablesM = new ListTablesRequestProtocolMarshaller(protocol)
  protected val putItemM = new PutItemRequestProtocolMarshaller(protocol)
  protected val queryM = new QueryRequestProtocolMarshaller(protocol)
  protected val scanM = new ScanRequestProtocolMarshaller(protocol)
  protected val transactGetItemsM = new TransactGetItemsRequestProtocolMarshaller(protocol)
  protected val transactWriteItemsM = new TransactWriteItemsRequestProtocolMarshaller(protocol)
  protected val updateItemM = new UpdateItemRequestProtocolMarshaller(protocol)
  protected val updateTableM = new UpdateTableRequestProtocolMarshaller(protocol)
  protected val updateTimeToLiveM = new UpdateTimeToLiveRequestProtocolMarshaller(protocol)

  protected val batchGetItemU = protocol.createResponseHandler(meta, new BatchGetItemResultJsonUnmarshaller)
  protected val batchWriteItemU = protocol.createResponseHandler(meta, new BatchWriteItemResultJsonUnmarshaller)
  protected val createTableU = protocol.createResponseHandler(meta, new CreateTableResultJsonUnmarshaller)
  protected val deleteItemU = protocol.createResponseHandler(meta, new DeleteItemResultJsonUnmarshaller)
  protected val deleteTableU = protocol.createResponseHandler(meta, new DeleteTableResultJsonUnmarshaller)
  protected val describeLimitsU = protocol.createResponseHandler(meta, new DescribeLimitsResultJsonUnmarshaller)
  protected val describeTableU = protocol.createResponseHandler(meta, new DescribeTableResultJsonUnmarshaller)
  protected val describeTimeToLiveU =
    protocol.createResponseHandler(meta, new DescribeTimeToLiveResultJsonUnmarshaller)
  protected val getItemU = protocol.createResponseHandler(meta, new GetItemResultJsonUnmarshaller)
  protected val listTablesU = protocol.createResponseHandler(meta, new ListTablesResultJsonUnmarshaller)
  protected val putItemU = protocol.createResponseHandler(meta, new PutItemResultJsonUnmarshaller)
  protected val queryU = protocol.createResponseHandler(meta, new QueryResultJsonUnmarshaller)
  protected val scanU = protocol.createResponseHandler(meta, new ScanResultJsonUnmarshaller)
  protected val transactGetItemsU = protocol.createResponseHandler(meta, new TransactGetItemsResultJsonUnmarshaller)
  protected val transactWriteItemsU = protocol.createResponseHandler(meta, new TransactWriteItemsResultJsonUnmarshaller)
  protected val updateItemU = protocol.createResponseHandler(meta, new UpdateItemResultJsonUnmarshaller)
  protected val updateTableU = protocol.createResponseHandler(meta, new UpdateTableResultJsonUnmarshaller)
  protected val updateTimeToLiveU = protocol.createResponseHandler(meta, new UpdateTimeToLiveResultJsonUnmarshaller)

}
