/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.lang

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.stream.alpakka.dynamodb.{AwsOp, DynamoSettings}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ExampleSpec
    extends TestKit(ActorSystem("ExampleSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val materializer: Materializer = ActorMaterializer()

  var dynamoClient: DynamoClient = _

  override def beforeAll() = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
    val settings = DynamoSettings(system)
    dynamoClient = DynamoClient(settings)
  }

  override def afterAll(): Unit = shutdown()

  "DynamoDB Client" should {

    "provide a simple usage example" in {

      //#client-construct
      implicit val system: ActorSystem = ActorSystem()
      implicit val materializer: Materializer = ActorMaterializer()

      val settings = DynamoSettings(system)
      val dynamoClient = DynamoClient(settings)
      //#client-construct

      //##simple-request
      import DynamoImplicits._
      val listTablesResult: Future[ListTablesResult] =
        dynamoClient.single(new ListTablesRequest())
      //##simple-request

      Await.result(listTablesResult, 5.seconds)

      TestKit.shutdownActorSystem(system)

    }

    "allow multiple requests - explicit types" in {
      import DynamoImplicits._
      val source = Source
        .single[AwsOp](new CreateTableRequest().withTableName("testTable"))
        .via(dynamoClient.flow)
        .map(_.asInstanceOf[CreateTableResult]) // <-- this is not very intuitive
        .map[AwsOp]( // <-- this is required to trigger the following implicit conversion, which takes some time to find out as well
          result => new DescribeTableRequest().withTableName(result.getTableDescription.getTableName)
        )
        .via(dynamoClient.flow)
        .map(_.asInstanceOf[DescribeTableResult])
        .map(result => result.getTable.getItemCount)
      val streamCompletion = source.runWith(Sink.seq)
      streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
    }
  }

  "allow multiple requests" in {
    //##flow
    import DynamoImplicits._
    val source: Source[String, NotUsed] = Source
      .single(new CreateTableRequest().withTableName("testTable"))
      .map(_.toOp) // converts to corresponding AwsOp
      .via(dynamoClient.flow)
      .map(_.getTableDescription.getTableArn)
    //##flow
    val streamCompletion = source.runWith(Sink.seq)
    streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
  }

  "allow multiple requests - single source" in {
    import DynamoImplicits._
    val source: Source[lang.Long, NotUsed] = dynamoClient
      .source(new CreateTableRequest().withTableName("testTable")) // creating a source from a single req is common enough to warrant a utility function
      .map(result => new DescribeTableRequest().withTableName(result.getTableDescription.getTableName))
      .map(_.toOp)
      .via(dynamoClient.flow)
      .map(result => result.getTable.getItemCount)
    val streamCompletion = source.runWith(Sink.seq)
    streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
  }

  "provide a paginated requests example" in {
    import DynamoImplicits._

    //##paginated
    val scanPages: Source[ScanResult, NotUsed] =
      dynamoClient.source(new ScanRequest().withTableName("testTable"))
    //##paginated
    val streamCompletion = scanPages.runWith(Sink.seq)
    streamCompletion.failed.futureValue shouldBe a[AmazonDynamoDBException]
  }

}
