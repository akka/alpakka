/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.testkit.{ SocketUtil, TestKit }
import org.scalatest._

class ItemSpec extends TestKit(ActorSystem("ItemSpec")) with AsyncWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val settings = DynamoSettings(system).copy(port = SocketUtil.temporaryServerAddress().getPort)
  val localDynamo = new LocalDynamo(settings)

  val client = DynamoClient(settings)

  override def beforeAll(): Unit = localDynamo.start()

  override def afterAll(): Unit = {
    localDynamo.stop()
    system.terminate()
  }

  "DynamoDB Client" should {

    import ItemSpecOps._
    import DynamoImplicits._

    "1) list zero tables" in {
      client.single(listTablesRequest).map(_.getTableNames.size shouldBe 0)
    }

    "2) create a table" in {
      client.single(createTableRequest).map(_.getTableDescription.getTableStatus shouldBe "ACTIVE")
    }

    "3) find a new table" in {
      client.single(listTablesRequest).map(_.getTableNames.size shouldBe 1)
    }

    "4) put an item and read it back" in {
      client
        .single(test4PutItemRequest)
        .flatMap(_ => client.single(getItemRequest))
        .map(_.getItem.get("data").getS shouldEqual "test4data")
    }

    "5) put an item and read it back in a batch" in {
      client.single(batchWriteItemRequest).map(_.getUnprocessedItems.size() shouldEqual 0)
    }

    "6) delete an item" in {
      client.single(deleteItemRequest).flatMap(_ => client.single(getItemRequest)).map(_.getItem() shouldEqual null)
    }

  }

}
