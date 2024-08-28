/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.actor.ActorSystem
import org.scalatest.Ignore

@Ignore
class AzureIntegrationTest extends StorageIntegrationSpec {

  override protected implicit val system: ActorSystem = ActorSystem("AzureIntegrationTest")

}
