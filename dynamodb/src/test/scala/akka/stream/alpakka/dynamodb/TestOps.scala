/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._

trait TestOps {

  val tableName: String

  val keyCol = "kkey"
  val sortCol = "sort"

  def S(s: String) = AttributeValue.builder().s(s).build()
  def N(n: Int) = AttributeValue.builder().n(n.toString).build()
  def keyMap(hash: String, sort: Int): Map[String, AttributeValue] = Map(
    keyCol -> S(hash),
    sortCol -> N(sort)
  )

  def keyEQ(hash: String): Map[String, Condition] = Map(
    keyCol -> Condition
      .builder()
      .comparisonOperator(ComparisonOperator.EQ)
      .attributeValueList(S(hash))
      .build()
  )

  object common {
    val listTablesRequest = ListTablesRequest.builder().build()

    val createTableRequest = CreateTableRequest
      .builder()
      .tableName(tableName)
      .keySchema(
        KeySchemaElement.builder().attributeName(keyCol).keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName(sortCol).keyType(KeyType.RANGE).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName(keyCol).attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName(sortCol).attributeType(ScalarAttributeType.N).build()
      )
      .provisionedThroughput(
        ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()
      )
      .build()

    val describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build()

    val deleteTableRequest = DeleteTableRequest.builder().tableName(tableName).build()
  }

}

abstract class ItemSpecOps extends TestOps {

  override val tableName = "ItemSpecOps"

  val listTablesRequest = common.listTablesRequest

  val createTableRequest = common.createTableRequest

  val test4Data = "test4data"

  val test4PutItemRequest =
    PutItemRequest.builder().tableName(tableName).item((keyMap("A", 0) + ("data" -> S(test4Data))).asJava).build()

  val getItemRequest =
    GetItemRequest.builder().tableName(tableName).key(keyMap("A", 0).asJava).attributesToGet("data").build()

  val getItemMalformedRequest =
    GetItemRequest.builder().tableName(tableName).attributesToGet("data").build()

  val test5Data = "test5Data"

  val test5PutItemRequest =
    PutItemRequest.builder().tableName(tableName).item((keyMap("A", 1) + ("data" -> S(test5Data))).asJava).build()

  val batchWriteItemRequest = BatchWriteItemRequest
    .builder()
    .requestItems(
      Map(
        tableName ->
        List(
          WriteRequest
            .builder()
            .putRequest(PutRequest.builder().item((keyMap("B", 0) + ("data" -> S(test5Data))).asJava).build())
            .build(),
          WriteRequest
            .builder()
            .putRequest(PutRequest.builder().item((keyMap("B", 1) + ("data" -> S(test5Data))).asJava).build())
            .build()
        ).asJava
      ).asJava
    )
    .build()

  def batchWriteLargeItemRequest(from: Int, to: Int) =
    BatchWriteItemRequest
      .builder()
      .requestItems(
        Map(
          tableName ->
          (from to to).map { i =>
            // 400k is the of one write request
            WriteRequest
              .builder()
              .putRequest(
                PutRequest.builder().item((keyMap(i.toString, i) + ("data1" -> S("0123456789" * 39000))).asJava).build()
              )
              .build()
          }.asJava
        ).asJava
      )
      .build()

  def batchGetLargeItemRequest(from: Int, to: Int) =
    BatchGetItemRequest
      .builder()
      .requestItems(
        Map(
          tableName ->
          KeysAndAttributes
            .builder()
            .keys {
              (from to to).map { i =>
                Map(keyCol -> S(i.toString), sortCol -> N(i)).asJava
              }.asJava
            }
            .attributesToGet("data1")
            .build()
        ).asJava
      )
      .build()

  def batchGetItemRequest(items: java.util.Map[String, KeysAndAttributes]) =
    BatchGetItemRequest.builder().requestItems(items).build()

  val queryItemsRequest = QueryRequest
    .builder()
    .tableName(tableName)
    .keyConditions(keyEQ("B").asJava)
    .limit(1)
    .build()

  val deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(keyMap("A", 0).asJava).build()

  def test7PutItemRequest(n: Int) =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(keyMap("A", n).asJava)
      .build()

  val querySize =
    QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression(s"$keyCol = :k")
      .expressionAttributeValues(Map(":k" -> S("A")).asJava)
      .build()

  val test8Data = "test8Data"

  val transactPutItemsRequest = TransactWriteItemsRequest
    .builder()
    .transactItems(
      List(
        TransactWriteItem
          .builder()
          .put(Put.builder().tableName(tableName).item((keyMap("C", 0) + ("data" -> S(test8Data))).asJava).build())
          .build(),
        TransactWriteItem
          .builder()
          .put(Put.builder().tableName(tableName).item((keyMap("C", 1) + ("data" -> S(test8Data))).asJava).build())
          .build()
      ).asJava
    )
    .build()

  val transactGetItemsRequest = TransactGetItemsRequest
    .builder()
    .transactItems(
      List(
        TransactGetItem.builder().get(Get.builder().tableName(tableName).key(keyMap("C", 0).asJava).build()).build(),
        TransactGetItem.builder().get(Get.builder().tableName(tableName).key(keyMap("C", 1).asJava).build()).build()
      ).asJava
    )
    .build()

  val transactDeleteItemsRequest = TransactWriteItemsRequest
    .builder()
    .transactItems(
      List(
        TransactWriteItem
          .builder()
          .delete(Delete.builder().tableName(tableName).key(keyMap("C", 0).asJava).build())
          .build(),
        TransactWriteItem
          .builder()
          .delete(Delete.builder().tableName(tableName).key(keyMap("C", 1).asJava).build())
          .build()
      ).asJava
    )
    .build()

  val deleteTableRequest = common.deleteTableRequest
}

object ItemSpecOps extends ItemSpecOps

object TableSpecOps extends TestOps {

  override val tableName = "TableSpecOps"

  val createTableRequest = common.createTableRequest

  val listTablesRequest = common.listTablesRequest

  val describeTableRequest = common.describeTableRequest

  val newMaxLimit = 5L
  val describeLimitsRequest = DescribeLimitsRequest.builder().build()
  val updateTableRequest = UpdateTableRequest
    .builder()
    .tableName(tableName)
    .provisionedThroughput(
      ProvisionedThroughput.builder().writeCapacityUnits(newMaxLimit).readCapacityUnits(newMaxLimit).build()
    )
    .build()

  val describeTimeToLiveRequest = DescribeTimeToLiveRequest.builder().build()
  val updateTimeToLiveRequest = UpdateTimeToLiveRequest
    .builder()
    .tableName(tableName)
    .timeToLiveSpecification(TimeToLiveSpecification.builder().attributeName("expires").enabled(true).build())
    .build()

  val deleteTableRequest = common.deleteTableRequest

}
