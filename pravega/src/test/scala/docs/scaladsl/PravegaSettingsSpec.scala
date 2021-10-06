/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.net.URI
import akka.stream.alpakka.pravega.{
  PravegaBaseSpec,
  ReaderSettingsBuilder,
  TableReaderSettingsBuilder,
  TableWriterSettingsBuilder,
  WriterSettingsBuilder
}
import io.pravega.client.stream.Serializer
import io.pravega.client.stream.impl.UTF8StringSerializer
import org.scalatest.matchers.must.Matchers

import java.nio.ByteBuffer
import scala.concurrent.duration._
import io.pravega.client.tables.TableKey

class PravegaSettingsSpec extends PravegaBaseSpec with Matchers {

  implicit val serializer = new UTF8StringSerializer

  implicit val intSerializer = new Serializer[Int] {
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

  "Table settings builder" must {

    "build TableWriterSettings with programmatic customisation" in {
      //#table-writer-settings
      val tableWriterSettings = TableWriterSettingsBuilder[Int, String]
        .clientConfigBuilder(_.enableTlsToController(true)) // ClientConfig customization
        .withMaximumInflightMessages(5)
        .withSerializers(str => new TableKey(intSerializer.serialize(str.hashCode())))
        .build()
      //#table-writer-settings

      tableWriterSettings.maximumInflightMessages mustEqual 5
    }

    "build TableReaderSettings with programmatic customisation" in {
      //#table-reader-settings
      val tableReaderSettings = TableReaderSettingsBuilder[Int, String]
        .clientConfigBuilder(_.enableTlsToController(true)) // ClientConfig customization
        .withMaximumInflightMessages(5)
        .withMaxEntriesAtOnce(100)
        .withTableKey(str => new TableKey(intSerializer.serialize(str.hashCode())))
        .build()
      //#table-reader-settings

      tableReaderSettings.maximumInflightMessages mustEqual 5
      tableReaderSettings.maxEntriesAtOnce mustEqual 100
    }

  }

}
