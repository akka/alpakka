/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.dynamodb.{DynamoDbTest, ForAllAnyWordSpecContainer}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{FlowWithContext, SourceWithContext}

import scala.util.{Failure, Success, Try}
import akka.actor.ActorSystem

import akka.stream.alpakka.dynamodb.DynamoDbOp._
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers

class ExampleSpec
    extends TestKit(ActorSystem("ExampleSpec"))
    with ForAllAnyWordSpecContainer
    with DynamoDbTest
    with Matchers
    with ScalaFutures
    with LogCapturing {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  implicit var client: DynamoDbAsyncClient = _

  override def afterStart(): Unit = {
    client = dynamoDbAsyncClient
    super.afterStart()
  }

  "DynamoDB" should {

    "provide a simple usage example" in {

      // #simple-request
      val listTablesResult: Future[ListTablesResponse] =
        DynamoDb.single(ListTablesRequest.builder().build())
      // #simple-request

      listTablesResult.futureValue
    }

    "allow multiple requests" in {
      // #flow
      val source: Source[DescribeTableResponse, NotUsed] = Source
        .single(CreateTableRequest.builder().tableName("testTable").build())
        .via(DynamoDb.flow(parallelism = 1))
        .map(response => DescribeTableRequest.builder().tableName(response.tableDescription.tableName).build())
        .via(DynamoDb.flow(parallelism = 1))

      // #flow
      source.runWith(Sink.ignore).failed.futureValue
    }

    "flow with context" in {
      case class SomeContext()

      // #withContext
      val source: SourceWithContext[PutItemRequest, SomeContext, NotUsed] = // ???
        // #withContext
        SourceWithContext.fromTuples(
          Source.single(PutItemRequest.builder().build() -> SomeContext())
        )

      // #withContext

      val flow: FlowWithContext[PutItemRequest, SomeContext, Try[PutItemResponse], SomeContext, NotUsed] =
        DynamoDb.flowWithContext(parallelism = 1)

      val writtenSource: SourceWithContext[PutItemResponse, SomeContext, NotUsed] = source
        .via(flow)
        .map {
          case Success(response) => response
          case Failure(exception) => throw exception
        }
      // #withContext

      writtenSource.runWith(Sink.ignore).failed.futureValue
    }

    "allow multiple requests - single source" in {
      (for {
        create <- DynamoDb.single(CreateTableRequest.builder().tableName("testTable").build())
        describe <- DynamoDb.single(
          DescribeTableRequest.builder().tableName(create.tableDescription.tableName).build()
        )
      } yield describe.table.itemCount).failed.futureValue
    }

    "provide a paginated requests source" in {
      // #paginated
      val scanRequest = ScanRequest.builder().tableName("testTable").build()

      val scanPages: Source[ScanResponse, NotUsed] =
        DynamoDb.source(scanRequest)

      // #paginated
      scanPages.runWith(Sink.ignore).failed.futureValue
    }

    "provide a paginated flow" in {
      val scanRequest = ScanRequest.builder().tableName("testTable").build()
      // #paginated
      val scanPageInFlow: Source[ScanResponse, NotUsed] =
        Source
          .single(scanRequest)
          .via(DynamoDb.flowPaginated())
      // #paginated
      scanPageInFlow.runWith(Sink.ignore).failed.futureValue
    }
  }
}
