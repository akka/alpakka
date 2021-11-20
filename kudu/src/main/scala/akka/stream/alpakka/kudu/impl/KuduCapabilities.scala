/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.impl

import akka.stream.stage.StageLogging
import org.apache.kudu.client._
import org.apache.kudu.Schema
import org.apache.kudu.client.KuduTable

/**
 * INTERNAL API
 */
private trait KuduCapabilities {
  this: StageLogging =>

  protected def getOrCreateTable(kuduClient: KuduClient,
                                 tableName: String,
                                 schema: Schema,
                                 createTableOptions: CreateTableOptions): KuduTable =
    if (kuduClient.tableExists(tableName))
      kuduClient.openTable(tableName)
    else {
      kuduClient.createTable(tableName, schema, createTableOptions)
      log.info("Table {} created with columns: {}.", tableName, schema)
      kuduClient.openTable(tableName)
    }

}
