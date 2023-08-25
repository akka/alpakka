/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class GCSSettingsSpec extends AnyFlatSpec with Matchers with LogCapturing {
  "GCSSettings" should "create settings from application config" in {
    val endpointUrl = "https://storage.googleapis.com/"
    val basePath = "/storage/v1"

    val config = ConfigFactory.parseMap(
      Map(
        "endpoint-url" -> endpointUrl,
        "base-path" -> basePath
      ).asJava
    )

    val settings = GCSSettings(config)

    settings.endpointUrl shouldBe endpointUrl
    settings.basePath shouldBe basePath
  }

}
