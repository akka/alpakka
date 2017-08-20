/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model._
import org.scalatest._
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.{GetRecords, GetShardIterator}
import akka.stream.scaladsl.Source
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.JavaConverters._
import scala.concurrent.Future

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
  val client = DynamoClient(settings)

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

    "2) get the ARN of the stream" in {
      Given("a table with streams enabled")
      When("we describe the stream")
      val describeTableResult = client
        .single(describeTableRequest)
        .futureValue

      val arn = describeTableResult.getTable.getLatestStreamArn
      Then("the ARN should be defined")
      Option(arn).isDefined shouldBe true

      When("we put and delete some data")
      client.single(test4PutItemRequest).futureValue
      client.single(deleteItemRequest).futureValue

      Then("we can get the shards")
      val describeStreamResult = client.single(describeStreamRequest(arn)).futureValue
      val shards = describeStreamResult.getStreamDescription.getShards.asScala

      val recordsSource =
        Source.fromIterator(() => shards.toIterator).flatMapConcat { shard => // can we process shards in parallel?
          val parent = shard.getParentShardId
          println(s"parent $parent")
          Source
            .single(getShardIteratorRequest(streamArn = arn, shardId = shard.getShardId).toOp)
            .via(client.flow)
            .flatMapConcat { //results need to be in the same order
              result =>
                Source.unfoldAsync(Option(result.getShardIterator)) {
                  case None => Future.successful(None)
                  case Some(nextShardIterator) =>
                    client.single(getRecordsRequest(nextShardIterator)).map { result =>
                      Some((Option(result.getNextShardIterator), result.getRecords))
                    }
                }

            }
            .flatMapConcat(records => Source.fromIterator(() => records.asScala.toIterator))
        }

      recordsSource.runForeach(records => println(s"received $records"))
    }
//
//    "3) update data" in {
//      client.single(listTablesRequest).map(_.getTableNames.asScala.count(_ == tableName) shouldBe 1)
//    }

//    "4) get the shards and read stream records" in {
//      client
//        .single(test4PutItemRequest)
//        .flatMap(_ => client.single(getItemRequest))
//        .map(_.getItem.get("data").getS shouldEqual "test4data")
//    }
//
//    "5) put an item and read it back in a batch" in {
//      client.single(batchWriteItemRequest).map(_.getUnprocessedItems.size() shouldEqual 0)
//    }
//
//    "6) delete an item" in {
//      client.single(deleteItemRequest).flatMap(_ => client.single(getItemRequest)).map(_.getItem() shouldEqual null)
//    }

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

  def getShardIteratorRequest(streamArn: String, shardId: String) =
    new GetShardIteratorRequest()
      .withStreamArn(streamArn)
      .withShardId(shardId)
      .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)

  def getRecordsRequest(shardIterator: String) = new GetRecordsRequest().withShardIterator(shardIterator)
}
