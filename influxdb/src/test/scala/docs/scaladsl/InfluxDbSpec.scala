/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.influxdb.dto.{Point, Query, QueryResult}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.{InfluxDbReadSettings, InfluxDbWriteMessage}
import akka.stream.alpakka.influxdb.scaladsl.{InfluxDbSink, InfluxDbSource}
import akka.testkit.TestKit
import docs.javadsl.TestUtils._
import akka.stream.scaladsl.Sink

import scala.concurrent.duration._
import scala.collection.JavaConverters._

import docs.javadsl.TestConstants.{INFLUXDB_URL, PASSWORD, USERNAME}

class InfluxDbSpec extends WordSpec with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll with ScalaFutures {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  final val DatabaseName = this.getClass.getSimpleName

  implicit var influxDB: InfluxDB = _

  //#define-class
  override protected def beforeAll(): Unit = {
    influxDB = InfluxDBFactory.connect(INFLUXDB_URL, USERNAME, PASSWORD);
    influxDB.setDatabase(DatabaseName);
    influxDB.query(new Query("CREATE DATABASE " + DatabaseName, DatabaseName));
  }

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  override def beforeEach(): Unit =
    populateDatabase(influxDB, classOf[InfluxDbSpecCpu])

  override def afterEach() =
    cleanDatabase(influxDB, DatabaseName)

  "support typed source" in assertAllStagesStopped {
    val query = new Query("SELECT * FROM cpu", DatabaseName);
    val measurements =
      InfluxDbSource.typed(classOf[InfluxDbSpecCpu], InfluxDbReadSettings(), influxDB, query).runWith(Sink.seq)

    measurements.futureValue.map(_.getHostname) mustBe List("local_1", "local_2")
  }

  "InfluxDbFlow" should {

    "consume and publish measurements using typed" in assertAllStagesStopped {
      val query = new Query("SELECT * FROM cpu", DatabaseName);

      //#run-typed
      val f1 = InfluxDbSource
        .typed(classOf[InfluxDbSpecCpu], InfluxDbReadSettings(), influxDB, query)
        .map { cpu: InfluxDbSpecCpu =>
          {
            val clonedCpu = cpu.cloneAt(cpu.getTime.plusSeconds(60000))
            List(InfluxDbWriteMessage(clonedCpu))
          }
        }
        .runWith(InfluxDbSink.typed(classOf[InfluxDbSpecCpu]))

      f1.futureValue mustBe Done

      val f2 =
        InfluxDbSource.typed(classOf[InfluxDbSpecCpu], InfluxDbReadSettings(), influxDB, query).runWith(Sink.seq)

      val aFutVal = f2.futureValue
      f2.futureValue.length mustBe 4
    }

    "consume and publish measurements" in assertAllStagesStopped {
      val query = new Query("SELECT * FROM cpu", DatabaseName);
      //#run-query-result
      val f1 = InfluxDbSource(influxDB, query)
        .map(resultToPoints)
        .runWith(InfluxDbSink.create())

      f1.futureValue mustBe Done

      val f2 =
        InfluxDbSource.typed(classOf[InfluxDbSpecCpu], InfluxDbReadSettings(), influxDB, query).runWith(Sink.seq)

      f2.futureValue.length mustBe 4
    }

    def resultToPoints(queryResult: QueryResult): List[InfluxDbWriteMessage[Point, NotUsed]] = {
      val points = for {
        results <- queryResult.getResults.asScala
        series <- results.getSeries.asScala
        values <- series.getValues.asScala
      } yield (
        InfluxDbWriteMessage(resultToPoint(series, values))
      )
      points.toList
    }

  }

}
