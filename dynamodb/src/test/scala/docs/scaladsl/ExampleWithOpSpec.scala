/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.lang

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.stream.alpakka.dynamodb.{DynamoAttributes, DynamoClient, DynamoSettings}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class ExampleWithOpSpec
    extends TestKit(ActorSystem("ExampleSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val materializer: Materializer = ActorMaterializer()
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  override def beforeAll(): Unit = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  override def afterAll(): Unit = shutdown()

  "DynamoDB with Op" should {

    "provide a simple usage example with singleOp" in {
      //##simple-request
      val listTablesResult: Future[ListTablesResult] =
        DynamoDb.singleOp(new ListTablesRequest())
      //##simple-request

      listTablesResult.futureValue
    }

    "allow multiple requests with flowOp - explicit types" in assertAllStagesStopped {
      import akka.stream.alpakka.dynamodb.AwsOp._
      val tableName = "testTable"
      val createTableOp: CreateTable = new CreateTableRequest().withTableName(tableName)
      val describeTableOp: DescribeTable = new DescribeTableRequest().withTableName(tableName)

      val source = Source
        .single[CreateTable](createTableOp)
        .via(DynamoDb.flowOp(createTableOp))
        .map[DescribeTable](_ => describeTableOp)
        .via(DynamoDb.flowOp(describeTableOp))
        .map(_.getTable.getItemCount)
      val streamCompletion = source.runWith(Sink.seq)
      streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
    }

    "allow multiple requests with flowOp" in assertAllStagesStopped {
      //##flow
      import akka.stream.alpakka.dynamodb.AwsOp._
      val createTableOp: CreateTable = new CreateTableRequest().withTableName("testTable")

      val source: Source[String, NotUsed] = Source
        .single[CreateTable](createTableOp)
        .via(DynamoDb.flowOp(createTableOp))
        .map(_.getTableDescription.getTableArn)
      //##flow
      val streamCompletion = source.runWith(Sink.seq)
      streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
    }

    "allow multiple requests with sourceOp - single source" in assertAllStagesStopped {
      import akka.stream.alpakka.dynamodb.AwsOp._
      val tableName = "testTable"
      val createTableOp: CreateTable = new CreateTableRequest().withTableName(tableName)
      val describeTableOp: DescribeTable = new DescribeTableRequest().withTableName(tableName)

      val source: Source[lang.Long, NotUsed] = DynamoDb
        .sourceOp(createTableOp) // creating a source from a single req is common enough to warrant a utility function
        .map[DescribeTable](_ => describeTableOp)
        .via(DynamoDb.flowOp(describeTableOp))
        .map(_.getTable.getItemCount)
      val streamCompletion = source.runWith(Sink.seq)
      streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
    }

    "provide a paginated requests example with sourceOp" in assertAllStagesStopped {
      //##paginated
      val scanPages: Source[ScanResult, NotUsed] =
        DynamoDb.sourceOp(new ScanRequest().withTableName("testTable"))
      //##paginated
      val streamCompletion = scanPages.runWith(Sink.seq)
      streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
    }

    "use client from attributes with sourceOp" in assertAllStagesStopped {
      // #attributes
      val settings = DynamoSettings(system).withRegion("custom-region")
      val client = DynamoClient(settings)

      val source: Source[ListTablesResult, NotUsed] =
        DynamoDb
          .sourceOp(new ListTablesRequest())
          .withAttributes(DynamoAttributes.client(client))
      // #attributes

      source.runWith(Sink.head).futureValue
    }
  }
}
