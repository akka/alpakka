/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ibm.eventstore.scaladsl

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.testkit.TestKit

import com.ibm.event.catalog.TableSchema
import com.ibm.event.common.ConfigurationReader
import com.ibm.event.oltp.EventContext
import com.ibm.event.oltp.InsertResult
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

/**
 * This integration test can be run using a local installation of EventStore
 * The installer for EventStore can be obtained from:
 * https://www.ibm.com/us-en/marketplace/project-eventstore
 *
 * Note: Run each integration test (Java and Scala) one at the time
 *
 * Before running the test:
 * Change the host and port below in the function 'setEndpoint' to the EventStore
 * Change the host and port below in the function 'failureEndpoint' to a unresponsive host/port.
 */
@Ignore
class EventStoreSpec
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MustMatchers {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = mat.executionContext
  private val databaseName = "TESTDB"
  private val tableName = "TESTTABLE"
  private var eventContext: Option[EventContext] = None

  private def setEndpoint() =
    // #configure-endpoint
    ConfigurationReader.setConnectionEndpoints("127.0.0.1:5555")
  // #configure-endpoint

  private def setFailureEndpoint() =
    ConfigurationReader.setConnectionEndpoints("192.168.1.1:5555")

  private def tableSchema: TableSchema =
    TableSchema(
      tableName,
      StructType(
        Array(
          StructField("id", LongType, nullable = false),
          StructField("someInt", IntegerType, nullable = false),
          StructField("someString", StringType, nullable = false),
          StructField("someBoolean", BooleanType, nullable = false),
          StructField("someOtherBoolean", BooleanType, nullable = true)
        )
      ),
      shardingColumns = Seq("id"),
      pkColumns = Seq("id")
    )

  implicit val defaultPatience = PatienceConfig(timeout = 3.seconds, interval = 50.millis)

  override def beforeAll(): Unit = {
    setEndpoint()
    EventContext.dropDatabase(databaseName)
    eventContext = Some(EventContext.createDatabase(databaseName))
  }

  override def beforeEach(): Unit =
    eventContext.foreach(_.createTable(tableSchema))

  override def afterEach(): Unit = eventContext.foreach(_.dropTable(tableName))

  override def afterAll(): Unit = {
    EventContext.dropDatabase(databaseName)
    // #cleanup
    EventContext.cleanUp()
    // #cleanup
    TestKit.shutdownActorSystem(system)
  }

  "insert 3 rows into EventStore flow" in {

    //#insert-rows-using-flow
    val rows =
      List(
        Row(1L, 1, "Hello", true, false),
        Row(2L, 2, "Hello", false, null),
        Row(3L, 3, "Hello", true, true)
      )

    val insertionResultFuture: Future[immutable.Seq[InsertResult]] =
      Source(rows).via(EventStoreFlow(databaseName, tableName)).runWith(Sink.seq)
    //#insert-rows-using-flow
    val result = Await.result(insertionResultFuture, 3.seconds)

    result.size mustBe 3
    result.map(_.successful) mustBe Seq(true, true, true)

  }

  "insert 3 rows into EventStore sink" in {

    //#insert-rows
    val rows =
      List(
        Row(1L, 1, "Hello", true, false),
        Row(2L, 2, "Hello", false, null),
        Row(3L, 3, "Hello", true, true)
      )

    val insertionResultFuture = Source(rows).runWith(EventStoreSink(databaseName, tableName))
    //#insert-rows
    insertionResultFuture.futureValue mustBe Done

  }

  "verify that a insert fails if no host responds" in {

    setFailureEndpoint()

    val rows =
      List(
        Row(1L, 1, "Hello", true, false),
        Row(2L, 2, "Hello", false, null),
        Row(3L, 3, "Hello", true, true)
      )

    assertThrows[Exception] {
      Source(rows).runWith(EventStoreSink(databaseName, tableName))
    }

    setEndpoint()
  }

  "verify that a insert into a flow fails if no host responds" in {

    setFailureEndpoint()

    val rows =
      List(
        Row(1L, 1, "Hello", true, false),
        Row(2L, 2, "Hello", false, null),
        Row(3L, 3, "Hello", true, true)
      )

    assertThrows[Exception] {
      val insertionResultFuture: Future[immutable.Seq[InsertResult]] =
        Source(rows).via(EventStoreFlow(databaseName, tableName)).runWith(Sink.seq)
      Await.result(insertionResultFuture, 3.seconds)
    }

    setEndpoint()
  }
}
