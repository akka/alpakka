/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.testkit.TestKit
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class TableSpec
    extends TestKit(ActorSystem("TableSpec"))
    with ForAllAsyncWordSpecContainer
    with DynamoDbTest
    with Matchers {

  implicit val ec: ExecutionContext = system.dispatcher

  implicit var client: DynamoDbAsyncClient = _

  override def afterStart(): Unit = {
    client = dynamoDbAsyncClient
    super.afterStart()
  }

  "DynamoDB" should {

    import TableSpecOps._

    "1) create table" in {
      DynamoDb.single(createTableRequest).map(_.tableDescription.tableName shouldBe tableName)
    }

    "2) list tables" in {
      DynamoDb.single(listTablesRequest).map(_.tableNames.asScala should contain(tableName))
    }

    "3) describe table" in {
      DynamoDb.single(describeTableRequest).map(_.table.tableName shouldBe tableName)
    }

    "4) update table" in {
      for {
        describe <- DynamoDb.single(describeTableRequest)
        update <- DynamoDb.single(updateTableRequest)
      } yield {
        describe.table.provisionedThroughput.writeCapacityUnits shouldBe 10L
        update.tableDescription.provisionedThroughput.writeCapacityUnits shouldBe newMaxLimit
      }
    }

    // TODO: Enable this test when DynamoDB Local supports TTLs
    "5) update time to live" ignore {
      for {
        describe <- DynamoDb.single(describeTimeToLiveRequest)
        update <- DynamoDb.single(updateTimeToLiveRequest)
      } yield {
        describe.timeToLiveDescription.attributeName shouldBe empty
        update.timeToLiveSpecification.attributeName shouldBe "expires"
      }
    }

    "6) delete table" in {
      for {
        _ <- DynamoDb.single(deleteTableRequest)
        list <- DynamoDb.single(listTablesRequest)
      } yield list.tableNames.asScala should not contain (tableName)
    }

  }

}
