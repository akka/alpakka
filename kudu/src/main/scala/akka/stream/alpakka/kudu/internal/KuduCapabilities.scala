/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.internal

import akka.stream.stage.StageLogging
import org.apache.kudu.client._
import org.apache.kudu.Schema
import org.apache.kudu.client.KuduTable

private[internal] trait KuduCapabilities {
  this: StageLogging =>

  private[internal] def getOrCreateTable(kuduClient: KuduClient,
                                         tableName: String,
                                         schema: Schema,
                                         createTableOptions: CreateTableOptions): KuduTable = {
    val table =
      if (kuduClient.tableExists(tableName))
        kuduClient.openTable(tableName)
      else {
        kuduClient.createTable(tableName, schema, createTableOptions)
        log.info(s"Table $tableName created with columns: $schema.")
        kuduClient.openTable(tableName)
      }
    table
  }

}
