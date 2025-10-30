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
import org.scalatest.Ignore

// TODO: investigate how Azurite works, it is not even working with pure Java API
// `putBlob` operations fails with "Premature end of file." error with BadRequest.
@Ignore
class AzuriteIntegrationSpec extends StorageIntegrationSpec with ForAllTestContainer {

  override lazy val container: AzuriteContainer = new AzuriteContainer()

  override protected implicit val system: ActorSystem = ActorSystem("AzuriteIntegrationSpec")

  protected lazy val blobSettings: StorageSettings =
    StorageExt(system).settings("azurite").withEndPointUrl(container.getBlobHostAddress)

  override protected def getDefaultAttributes: Attributes = StorageAttributes.settings(blobSettings)
}
