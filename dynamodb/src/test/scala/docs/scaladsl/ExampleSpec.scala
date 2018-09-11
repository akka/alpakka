/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.stream.alpakka.dynamodb.{AwsOp, DynamoSettings}
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ExampleSpec extends TestKit(ActorSystem("ExampleSpec")) with WordSpecLike with Matchers with BeforeAndAfterAll {

  val settings = DynamoSettings(system)

  override def beforeAll() = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  "DynamoDB Client" should {

    "provide a simple usage example" in {

      //#init-client
      implicit val system: ActorSystem = ActorSystem()
      implicit val materializer: Materializer = ActorMaterializer()
      //#init-client

      //#client-construct
      val settings = DynamoSettings(system)
      val client = DynamoClient(settings)
      //#client-construct

      //##simple-request
      import DynamoImplicits._
      val listTablesResult: Future[ListTablesResult] = client.single(new ListTablesRequest())
      //##simple-request

      Await.result(listTablesResult, 5.seconds)

      system.terminate()

    }

    "allow multiple requests - current api" in {
      implicit val system = ActorSystem()
      implicit val materializer = ActorMaterializer()

      val settings = DynamoSettings(system)
      val client = DynamoClient(settings)

      import DynamoImplicits._
      Source
        .single[AwsOp](new CreateTableRequest().withTableName("testTable"))
        .via(client.flow)
        .map(_.asInstanceOf[CreateTable#B]) // <-- this is not very intuitive
        .map[AwsOp]( // <-- this is required to trigger the following implicit conversion, which takes some time to find out as well
          result => new DescribeTableRequest().withTableName(result.getTableDescription.getTableName)
        )
        .via(client.flow)
        .map(_.asInstanceOf[DescribeTable#B])
        .map(result => result.getTable.getItemCount)
    }
  }

  "allow multiple requests - proposal" in {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val settings = DynamoSettings(system)
    val client = DynamoClient(settings)

    import DynamoImplicits._
    //##flow
    Source
      .single(new CreateTableRequest().withTableName("testTable").toOp)
      .via(client.flow)
      .map(_.getTableDescription.getTableArn)
    //##flow
  }

  "allow multiple requests - proposal - single source" in {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val settings = DynamoSettings(system)
    val client = DynamoClient(settings)

    import DynamoImplicits._
    client
      .source(new CreateTableRequest().withTableName("testTable")) // creating a source from a single req is common enough to warrant a utility function
      .map(result => new DescribeTableRequest().withTableName(result.getTableDescription.getTableName).toOp)
      .via(client.flow)
      .map(result => result.getTable.getItemCount)

  }

  "provide a paginated requests example" in {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val settings = DynamoSettings(system)
    val client = DynamoClient(settings)

    import DynamoImplicits._

    //##paginated
    val scanPages: Source[ScanResult, NotUsed] =
      client.source(new ScanRequest().withTableName("testTable"))
    //##paginated

    system.terminate()
  }

}
