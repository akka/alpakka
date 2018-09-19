/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDbExternal
import akka.testkit.TestKit
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}

import scala.collection.JavaConverters._

class TableSpec extends TestKit(ActorSystem("TableSpec")) with AsyncWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val settings = DynamoSettings(system)

  override def beforeAll() = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  "DynamoDB with external client" should {

    import TableSpecOps._
    import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits._

    implicit val client = DynamoClient(settings)

    "1) create table" in {
      DynamoDbExternal.single(createTableRequest).map(_.getTableDescription.getTableName shouldEqual tableName)
    }

    "2) list tables" in {
      DynamoDbExternal.single(listTablesRequest).map(_.getTableNames.asScala.count(_ == tableName) shouldEqual 1)
    }

    "3) describe table" in {
      DynamoDbExternal.single(describeTableRequest).map(_.getTable.getTableName shouldEqual tableName)
    }

    "4) update table" in {
      DynamoDbExternal
        .single(describeTableRequest)
        .map(_.getTable.getProvisionedThroughput.getWriteCapacityUnits shouldEqual 10L)
        .flatMap(_ => DynamoDbExternal.single(updateTableRequest))
        .map(_.getTableDescription.getProvisionedThroughput.getWriteCapacityUnits shouldEqual newMaxLimit)
    }

    // TODO: Enable this test when DynamoDB Local supports TTLs
    "5) update time to live" ignore {
      DynamoDbExternal
        .single(describeTimeToLiveRequest)
        .map(_.getTimeToLiveDescription.getAttributeName shouldEqual null)
        .flatMap(_ => DynamoDbExternal.single(updateTimeToLiveRequest))
        .map(_.getTimeToLiveSpecification.getAttributeName shouldEqual "expires")
    }

    "6) delete table" in {
      DynamoDbExternal
        .single(deleteTableRequest)
        .flatMap(_ => DynamoDbExternal.single(listTablesRequest))
        .map(_.getTableNames.asScala.count(_ == tableName) shouldEqual 0)
    }

  }

}
