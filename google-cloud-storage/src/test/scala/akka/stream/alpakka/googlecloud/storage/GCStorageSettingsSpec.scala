/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpecLike, Matchers}
import scala.collection.JavaConverters._

class GCStorageSettingsSpec extends FlatSpecLike with Matchers {
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
