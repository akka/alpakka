/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import java.net.URI

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.collection.JavaConverters._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

class TableSpec extends TestKit(ActorSystem("TableSpec")) with AsyncWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val client: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .region(Region.AWS_GLOBAL)
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
    .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
    .endpointOverride(new URI("http://localhost:8001/"))
    .build()

  override def afterAll(): Unit = {
    client.close()
    shutdown()
    super.afterAll()
  }

  "DynamoDB" should {

    import TableSpecOps._

    "1) create table" in assertAllStagesStopped {
      DynamoDb.single(createTableRequest).map(_.tableDescription.tableName shouldBe tableName)
    }

    "2) list tables" in assertAllStagesStopped {
      DynamoDb.single(listTablesRequest).map(_.tableNames.asScala should contain(tableName))
    }

    "3) describe table" in assertAllStagesStopped {
      DynamoDb.single(describeTableRequest).map(_.table.tableName shouldBe tableName)
    }

    "4) update table" in assertAllStagesStopped {
      for {
        describe <- DynamoDb.single(describeTableRequest)
        update <- DynamoDb.single(updateTableRequest)
      } yield {
        describe.table.provisionedThroughput.writeCapacityUnits shouldBe 10L
        update.tableDescription.provisionedThroughput.writeCapacityUnits shouldBe newMaxLimit
      }
    }

    // TODO: Enable this test when DynamoDB Local supports TTLs
    "5) update time to live" ignore assertAllStagesStopped {
      for {
        describe <- DynamoDb.single(describeTimeToLiveRequest)
        update <- DynamoDb.single(updateTimeToLiveRequest)
      } yield {
        describe.timeToLiveDescription.attributeName shouldBe empty
        update.timeToLiveSpecification.attributeName shouldBe "expires"
      }
    }

    "6) delete table" in assertAllStagesStopped {
      for {
        _ <- DynamoDb.single(deleteTableRequest)
        list <- DynamoDb.single(listTablesRequest)
      } yield list.tableNames.asScala should not contain tableName
    }

  }

}
