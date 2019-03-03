/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
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
}
