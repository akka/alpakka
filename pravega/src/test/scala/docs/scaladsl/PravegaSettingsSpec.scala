/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.net.URI
import akka.stream.alpakka.pravega.{
  PravegaAkkaSpecSupport,
  ReaderSettingsBuilder,
  TableWriterSettingsBuilder,
  WriterSettingsBuilder
}
import akka.testkit.TestKit

import io.pravega.client.stream.Serializer
import io.pravega.client.stream.impl.UTF8StringSerializer
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.ByteBuffer
import scala.concurrent.duration._

class PravegaSettingsSpec extends AnyWordSpec with PravegaAkkaSpecSupport with Matchers {

  val serializer = new UTF8StringSerializer

  val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

  "ReaderSettingsBuilder" must {

    "build ReaderSettings with programmatic customization" in {
      //#reader-settings
      val readerSettings = ReaderSettingsBuilder(system)
        .clientConfigBuilder(
          _.controllerURI(new URI("pravegas://localhost:9090")) // ClientConfig customization
            .enableTlsToController(true)
            .enableTlsToSegmentStore(true)
        )
        .readerConfigBuilder(_.disableTimeWindows(true)) //ReaderConfig customization
        .withTimeout(3.seconds)
        .withSerializer(new UTF8StringSerializer)
      //#reader-settings

      readerSettings.timeout mustEqual 3000
      readerSettings.clientConfig.isEnableTlsToController mustBe true
      readerSettings.clientConfig.isEnableTls mustBe true

    }
  }
  "WriterSettingsBuilder" must {
    "build WriterSettings with programmatic customisation" in {

      //#writer-settings
      val writerSettinds = WriterSettingsBuilder(system)
        .clientConfigBuilder(_.enableTlsToController(true)) // ClientConfig customization
        .eventWriterConfigBuilder(_.backoffMultiple(5)) //EventWriterConfig customization
        .withMaximumInflightMessages(5)
        .withKeyExtractor((str: String) => str.substring(0, 2))
        .withSerializer(new UTF8StringSerializer)
      //#writer-settings

      writerSettinds.maximumInflightMessages mustEqual 5

    }

  }

  "TableWriterSettingsBuilder" must {
    case class Person(id: Int, firstname: String)

    "build TableWriterSettings with programmatic customisation" in {
      //#table-writer-settings
      val tableWriterSettings = TableWriterSettingsBuilder[Int, String](system)
        .clientConfigBuilder(_.enableTlsToController(true)) // ClientConfig customization
        .withMaximumInflightMessages(5)
        .withSerializers(intSerializer, serializer)
      //#table-writer-settings

      tableWriterSettings.maximumInflightMessages mustEqual 5
    }
  }

  override protected def beforeAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.beforeAll()
  }
}
