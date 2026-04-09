/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.actor.ActorSystem
import akka.stream.Attributes
import com.dimafeng.testcontainers.ForAllTestContainer

class AzuriteIntegrationSpec extends StorageIntegrationSpec with ForAllTestContainer {

  override lazy val container: AzuriteContainer = new AzuriteContainer()

  override protected implicit val system: ActorSystem = ActorSystem("AzuriteIntegrationSpec")

  protected lazy val blobSettings: StorageSettings =
    StorageExt(system).settings("azurite").withEndPointUrl(container.getBlobHostAddress)

  override protected def getDefaultAttributes: Attributes = StorageAttributes.settings(blobSettings)
}
