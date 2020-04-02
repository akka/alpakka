/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kudu

import akka.actor.{ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import org.apache.kudu.client.KuduClient

/**
 * Manages one [[org.apache.kudu.client.KuduClient]] per `ActorSystem`.
 */
final class KuduClientExt private (sys: ExtendedActorSystem) extends Extension {
  val client = {
    val masterAddress = sys.settings.config.getString("alpakka.kudu.master-address")
    new KuduClient.KuduClientBuilder(masterAddress).build
  }

  sys.registerOnTermination(client.shutdown())
}

object KuduClientExt extends ExtensionId[KuduClientExt] with ExtensionIdProvider {
  override def lookup = KuduClientExt
  override def createExtension(system: ExtendedActorSystem) = new KuduClientExt(system)

  /**
   * Get the Kudu Client extension with the classic actors API.
   * Java API.
   */
  override def get(system: akka.actor.ActorSystem): KuduClientExt = super.get(system)

  /**
   * Get the Kudu Client extension with the new actors API.
   * Java API.
   */
  override def get(system: ClassicActorSystemProvider): KuduClientExt = super.get(system)
}
