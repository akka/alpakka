/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.net.URI

import akka.stream.alpakka.pravega.{PravegaAkkaSpecSupport, ReaderSettingsBuilder, WriterSettingsBuilder}
import akka.testkit.TestKit
import io.pravega.client.stream.impl.UTF8StringSerializer
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PravegaSettingsSpec extends AnyWordSpec with PravegaAkkaSpecSupport with Matchers {

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
        .withSerializer(new UTF8StringSerializer)
      //#reader-settings

      readerSettings.timeout mustEqual 5000
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

  override protected def beforeAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.beforeAll()
  }
}
