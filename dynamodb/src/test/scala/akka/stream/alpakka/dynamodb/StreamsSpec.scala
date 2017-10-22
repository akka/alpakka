/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.JavaConverters._

class StreamsSpec
    extends TestKit(ActorSystem("ItemSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with GivenWhenThen
    with ScalaFutures {
  import DynamoImplicits._
  import StreamsSpecOps._

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))
  val settings = DynamoSettings(system)
  implicit val client = DynamoClient(settings)

  override def beforeAll() = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
    client.single(createTableRequest).futureValue
  }

  override def afterAll() =
    client
      .single(deleteTableRequest)
      .futureValue

  "DynamoDB Client" should {

    "get the stream of changes to a table" in {
      Given("a table with streams enabled")
      When("we put and delete some data")
      client.single(test4PutItemRequest).futureValue
      client.single(deleteItemRequest).futureValue

      And("create a stream of records for that table")

      val records = Streams
        .records(StreamsSpecOps.tableName)

      Then("we should receive an INSERT and REMOVE")
      records
        .map(_.getEventName)
        .runWith(TestSink.probe)
        .request(2)
        .expectNext("INSERT", "REMOVE")
    }
  }

}

object StreamsSpecOps extends TestOps {

  override val tableName = "StreamsSpecOps"

  val createTableRequest = {
    val streamSpecification =
      new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
    common.createTableRequest.withStreamSpecification(streamSpecification)
  }

  val listTablesRequest = common.listTablesRequest

  val describeTableRequest = common.describeTableRequest

  val deleteTableRequest = common.deleteTableRequest

  val test4Data = "test4data"

  val test4PutItemRequest =
    new PutItemRequest().withTableName(tableName).withItem((keyMap("A", 0) + ("data" -> S(test4Data))).asJava)

  val getItemRequest =
    new GetItemRequest().withTableName(tableName).withKey(keyMap("A", 0).asJava).withAttributesToGet("data")

  val deleteItemRequest = new DeleteItemRequest().withTableName(tableName).withKey(keyMap("A", 0).asJava)

  def describeStreamRequest(streamArn: String) = new DescribeStreamRequest().withStreamArn(streamArn)

}
