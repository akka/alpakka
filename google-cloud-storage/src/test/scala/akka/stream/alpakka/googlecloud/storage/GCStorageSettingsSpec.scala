/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.collection.JavaConverters._

class GCStorageSettingsSpec extends AnyFlatSpec with Matchers with LogCapturing {
  "GCStorageSettings" should "create settings from application config" in {
    val projectId = "projectId"
    val clientEmail = "clientEmail"
    val privateKey = "privateKey"
    val baseUrl = "http://base"
    val basePath = "/path"
    val tokenUrl = "http://token"
    val tokenScope = "everything"

    val config = ConfigFactory.parseMap(
      Map(
        "project-id" -> projectId,
        "client-email" -> clientEmail,
        "private-key" -> privateKey,
        "base-url" -> baseUrl,
        "base-path" -> basePath,
        "token-url" -> tokenUrl,
        "token-scope" -> tokenScope
      ).asJava
    )

    @nowarn("msg=deprecated")
    val settings = GCStorageSettings(config)

    settings.projectId shouldBe projectId
    settings.clientEmail shouldBe clientEmail
    settings.privateKey shouldBe privateKey
    settings.baseUrl shouldBe baseUrl
    settings.basePath shouldBe basePath
    settings.tokenUrl shouldBe tokenUrl
    settings.tokenScope shouldBe tokenScope
  }
}
