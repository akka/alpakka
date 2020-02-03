/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kudu

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.Attributes.Attribute
import org.apache.kudu.client.KuduClient

/**
 * Akka Stream attributes that are used when materializing Kudu stream blueprints.
 */
object KuduAttributes {

  /**
   * Kudu client to use for the stream
   */
  def client(client: KuduClient): Attributes = Attributes(new Client(client))

  final class Client @InternalApi private[KuduAttributes] (val client: KuduClient) extends Attribute
}
