/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.cassandra

final class CassandraServerMetaData(val clusterName: String, val dataCenter: String, val version: String) {

  val isVersion2: Boolean = version.startsWith("2.")

  override def toString: String =
    s"CassandraServerMetaData($clusterName,$dataCenter,$version)"

}
