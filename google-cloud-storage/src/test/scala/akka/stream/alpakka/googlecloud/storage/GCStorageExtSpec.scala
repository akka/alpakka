/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

class GCStorageExtSpec extends AnyFlatSpec with Matchers with LogCapturing {
  "GCStorageExt" should "reuse application config from actor system" in {
    val projectId = "projectId"
    val clientEmail = "clientEmail"
    val privateKey = "privateKey"
    val baseUrl = "http://base"
    val basePath = "/path"
    val tokenUrl = "http://token"
    val tokenScope = "everything"

    val config = ConfigFactory.parseMap(
      Map(
        "alpakka.google.cloud.storage.project-id" -> projectId,
        "alpakka.google.cloud.storage.client-email" -> clientEmail,
        "alpakka.google.cloud.storage.private-key" -> privateKey,
        "alpakka.google.cloud.storage.base-url" -> baseUrl,
        "alpakka.google.cloud.storage.base-path" -> basePath,
        "alpakka.google.cloud.storage.token-url" -> tokenUrl,
        "alpakka.google.cloud.storage.token-scope" -> tokenScope
      ).asJava
    )
    implicit val system: ActorSystem = ActorSystem.create("gcStorage", config)
    @nowarn("msg=deprecated")
    val ext = GCStorageExt(system)

    ext.settings.projectId shouldBe projectId
    ext.settings.clientEmail shouldBe clientEmail
    ext.settings.privateKey shouldBe privateKey
    ext.settings.baseUrl shouldBe baseUrl
    ext.settings.basePath shouldBe basePath
    ext.settings.tokenUrl shouldBe tokenUrl
    ext.settings.tokenScope shouldBe tokenScope
  }
}
