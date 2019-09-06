/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.net.URI

import akka.NotUsed
//#init-client
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

//#init-client
import akka.stream.alpakka.dynamodb.DynamoDbOp._
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
//#init-client
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

//#init-client
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class ExampleSpec
    extends TestKit(ActorSystem("ExampleSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  //#init-client
  implicit val materializer: Materializer = ActorMaterializer()

  implicit val client: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .region(Region.AWS_GLOBAL)
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
    .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
    .endpointOverride(new URI("http://localhost:8001/"))
    .build()

  system.registerOnTermination(client.close())

  //#init-client

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  "DynamoDB" should {

    "provide a simple usage example" in {

      //##simple-request
      val listTablesResult: Future[ListTablesResponse] =
        DynamoDb.single(ListTablesRequest.builder().build())
      //##simple-request

      listTablesResult.futureValue
    }

    "allow multiple requests" in assertAllStagesStopped {
      //##flow
      val source: Source[DescribeTableResponse, NotUsed] = Source
        .single(CreateTableRequest.builder().tableName("testTable").build())
        .via(DynamoDb.flow(1))
        .map(response => DescribeTableRequest.builder().tableName(response.tableDescription.tableName).build())
        .via(DynamoDb.flow(1))

      //##flow
      source.runWith(Sink.ignore).failed.futureValue
    }

    "allow multiple requests - single source" in assertAllStagesStopped {
      (for {
        create <- DynamoDb.single(CreateTableRequest.builder().tableName("testTable").build())
        describe <- DynamoDb.single(
          DescribeTableRequest.builder().tableName(create.tableDescription.tableName).build()
        )
      } yield describe.table.itemCount).failed.futureValue
    }

    "provide a paginated requests example" in assertAllStagesStopped {
      //##paginated
      val scanPages: Source[ScanResponse, NotUsed] =
        DynamoDb.source(ScanRequest.builder().tableName("testTable").build())
      //##paginated
      scanPages.runWith(Sink.ignore).failed.futureValue
    }
  }
}
