/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.actor.ActorSystem
import akka.stream.Attributes
import com.dimafeng.testcontainers.ForAllTestContainer
import com.typesafe.config.ConfigFactory
import org.scalatest.Ignore

import scala.concurrent.duration._
import scala.concurrent.Await

// TODO: investigate how Azurite works, it is not even working with pure Java API
// `putBlob` operations fails with "remature end of file."
@Ignore
class AzuriteIntegrationSpec extends StorageIntegrationSpec with ForAllTestContainer {

  override lazy val container: AzuriteContainer = new AzuriteContainer()

  override protected implicit val system: ActorSystem =
    ActorSystem("StorageIntegrationSpec", ConfigFactory.load("application-azurite").withFallback(ConfigFactory.load()))

  protected lazy val blobSettings: StorageSettings = StorageSettings().withEndPointUrl(container.getBlobHostAddress)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val eventualDone = createContainer(defaultContainerName)
    Await.result(eventualDone, 10.seconds)
  }

  override protected def getDefaultAttributes: Attributes = StorageAttributes.settings(blobSettings)
}
