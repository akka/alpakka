/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.druid

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.time.Clock

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.druid.scaladsl.Tranquilizer
import akka.stream.scaladsl.Source
import com.metamx.tranquility.config.TranquilityConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.scalatest.WordSpec
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import scala.language.postfixOps
import scala.concurrent.Await

class DruidITTestSuite extends WordSpec {

  val log = LoggerFactory.getLogger("akka.druid")

  implicit def readBytes(res: String): ByteBuffer =
    ByteBuffer.wrap(Files.readAllBytes(Paths.get(getClass.getResource(res).toURI)))

  val druidIT = sys.env.get("DRUID_ZOOKEEPER_TEST").flatMap { zk =>
    val conf = for {
      conf <- Try(ConfigFactory.load("test-reactive-druid.conf"))
      props <- Try(conf.getObject("properties"))
      zkc <- Try(props.get("zookeeper.connect"))
    } yield (conf, zkc.render())

    conf match {
      case Success((conf, zk)) =>
        log.info(s"Sending event to $zk")
        Some((conf, zk))

      case Failure(e) =>
        log.error("Could not parse druid configuration.")
        None
    }
  }

  "Reactive Druid " should {

    druidIT.foreach {
      case (conf, zk) =>
        s"send messages to druid with through stages (zk: $zk)" in {

          stages(conf)

        }
    }

    implicit def tranquilityConfig(config: Config) =
      TranquilityConfig.read(
        new ByteArrayInputStream(
          config.root().render(ConfigRenderOptions.concise()) getBytes (Charset.forName("UTF-8"))
        )
      )

    def buildTestEvent(i: Int): TestEvent =
      TestEvent(TimeUtils.timestamp2ISODateTime(Clock.systemUTC().instant()), s"page_$i", 1, List("un", "deux"))

    def stages(conf: Config): Unit = {
      implicit val actorSystem = ActorSystem("test")
      implicit val materializer = ActorMaterializer()

      //#tranquilizer-settings
      val config = TranquilizerSettings(
        dataSourceConfig = conf.getDataSource("test"),
        timestamper = DruidTestCodec.timestamper,
        objectWriter = DruidTestCodec.testWriter,
        partitioner = DruidTestCodec.partitioner
      )
      //#tranquilizer-settings

      //#druid-sink
      val sink = Tranquilizer.sink[TestEvent](config)
      //#druid-sink

      val f = Source(1 to 20).map(buildTestEvent).runWith(sink)

      Await.ready(f, 10 seconds)

    }

  }

}
