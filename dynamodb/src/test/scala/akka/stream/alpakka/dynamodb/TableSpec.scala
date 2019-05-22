/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}

import scala.collection.JavaConverters._

class TableSpec extends TestKit(ActorSystem("TableSpec")) with AsyncWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  override def beforeAll() = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  "DynamoDB" should {

    import TableSpecOps._

    "1) create table" in assertAllStagesStopped {
      DynamoDb.singleOp(createTableRequest).map(_.getTableDescription.getTableName shouldEqual tableName)
    }

    "2) list tables" in assertAllStagesStopped {
      DynamoDb.singleOp(listTablesRequest).map(_.getTableNames.asScala.count(_ == tableName) shouldEqual 1)
    }

    "3) describe table" in assertAllStagesStopped {
      DynamoDb.singleOp(describeTableRequest).map(_.getTable.getTableName shouldEqual tableName)
    }

    "4) update table" in assertAllStagesStopped {
      DynamoDb
        .singleOp(describeTableRequest)
        .map(_.getTable.getProvisionedThroughput.getWriteCapacityUnits shouldEqual 10L)
        .flatMap(_ => DynamoDb.singleOp(updateTableRequest))
        .map(_.getTableDescription.getProvisionedThroughput.getWriteCapacityUnits shouldEqual newMaxLimit)
    }

    // TODO: Enable this test when DynamoDB Local supports TTLs
    "5) update time to live" ignore assertAllStagesStopped {
      DynamoDb
        .singleOp(describeTimeToLiveRequest)
        .map(_.getTimeToLiveDescription.getAttributeName shouldEqual null)
        .flatMap(_ => DynamoDb.singleOp(updateTimeToLiveRequest))
        .map(_.getTimeToLiveSpecification.getAttributeName shouldEqual "expires")
    }

    "6) delete table" in assertAllStagesStopped {
      DynamoDb
        .singleOp(deleteTableRequest)
        .flatMap(_ => DynamoDb.singleOp(listTablesRequest))
        .map(_.getTableNames.asScala.count(_ == tableName) shouldEqual 0)
    }

  }

}
