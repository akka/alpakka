/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ibm.eventstore

import com.typesafe.config.Config

final case class EventStoreConfiguration(host: String,
                                         port: Short,
                                         databaseName: String,
                                         tableName: String,
                                         parallelism: Int = 1) {

  def endpoint = s"$host:$port"

}

object EventStoreConfiguration {
  def apply(config: Config): EventStoreConfiguration =
    EventStoreConfiguration(
      host = config.getString("akka.stream.alpakka.ibm.eventstore.host"),
      port = config.getInt("akka.stream.alpakka.ibm.eventstore.port").toShort,
      databaseName = config.getString("akka.stream.alpakka.ibm.eventstore.database-name"),
      tableName = config.getString("akka.stream.alpakka.ibm.eventstore.table-name"),
      parallelism = config.getInt("akka.stream.alpakka.ibm.eventstore.parallelism")
    )
}
