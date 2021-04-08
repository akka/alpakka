/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.OverflowStrategy
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.googlecloud.logging.{HoverflySupport, WriteEntriesSettings}
import akka.stream.alpakka.googlecloud.logging.model.{LogEntry, LogSeverity, MonitoredResource, WriteEntriesRequest}
import akka.stream.scaladsl.Source
import akka.testkit.{TestDuration, TestKit}
import com.typesafe.config.ConfigFactory
import io.specto.hoverfly.junit.core.{HoverflyMode, SimulationSource}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.DefaultJsonProtocol._

import java.io.File
import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.util.matching.Regex

class CloudLoggingEntriesSpec
    extends TestKit(ActorSystem("CloudLoggingEntriesSpec", HoverflySupport.config.withFallback(ConfigFactory.load())))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with HoverflySupport
    with CloudLoggingEntries {

  override def beforeAll(): Unit = {
    super.beforeAll()
    system.settings.config.getString("alpakka.google.logging.test.hoverfly-mode") match {
      case "simulate" =>
        val resource = getClass.getClassLoader.getResource("CloudLoggingEntriesSpec.json")
        hoverfly.simulate(SimulationSource.url(resource))

        // Hack to get initial timestamp in simulation
        val timestampPattern = new Regex("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z""")
        val src = scala.io.Source.fromFile(resource.toURI)
        initialTimestamp = src.getLines().flatMap(timestampPattern.findAllIn).map(Instant.parse).min
        src.close()
      case "capture" =>
        hoverfly.resetMode(HoverflyMode.CAPTURE)
        println()
      case _ => throw new IllegalArgumentException
    }
  }

  override def afterAll() = {
    system.terminate()
    if (hoverfly.getMode == HoverflyMode.CAPTURE) {
      hoverfly.exportSimulation(new File("src/test/resources/CloudLoggingEntriesSpec.json").toPath)

      println()
    }
    super.afterAll()
  }

  // Incoming log entries must have timestamps that don't exceed the logs retention period in the past, and that don't exceed 24 hours in the future.
  var initialTimestamp = Instant.now()

  case class Payload(message: String)
  implicit val payloadFormat = jsonFormat1(Payload)

  val googleSettings = GoogleSettings()

  implicit val patience = PatienceConfig(10.seconds.dilated)
  val random = new Random(1234567890)

  "CloudLoggingEntries" should {
    "successfully send logs" in {

      val settings = WriteEntriesSettings(
        128,
        LogSeverity.Error,
        1.second,
        OverflowStrategy.dropHead,
        WriteEntriesRequest()
          .withLabel("spec", "CloudLoggingEntriesSpec")
          .withLogName(s"projects/${googleSettings.projectId}/logs/CloudLoggingEntriesSpec.log")
          .withResource(MonitoredResource.Global)
      )

      def mkLogEntry() =
        LogEntry(Payload(random.nextString(12)))
          .withSeverity(LogSeverity(random.nextInt(9) * 100))
          .withTimestamp(initialTimestamp.plusSeconds(random.nextInt(60)))
          .withLabel("entropy", random.nextString(12))

      Source
        .fromIterator(() => Iterator.continually(mkLogEntry()))
        .take(100)
        // Record the initial timestamp in the Hoverfly recording
        .concat(Source.single(mkLogEntry().withTimestamp(initialTimestamp)))
        .runWith(writeEntries(settings))
        .futureValue shouldBe Done
    }
  }

}
