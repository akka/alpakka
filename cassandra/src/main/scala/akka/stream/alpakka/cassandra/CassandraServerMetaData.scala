/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

final class CassandraServerMetaData(val clusterName: String, val dataCenter: String, val version: String) {

  val isVersion2: Boolean = version.startsWith("2.")

  override def toString: String =
    s"CassandraServerMetaData($clusterName,$dataCenter,$version)"

}
