/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu

import org.apache.kudu.client._
import org.apache.kudu.Schema
import org.apache.kudu.client.PartialRow

final case class KuduTableSettings[T](kuduClient: KuduClient,
                                      tableName: String,
                                      schema: Schema,
                                      createTableOptions: CreateTableOptions,
                                      converter: T => PartialRow)

object KuduTableSettings {
  def create[T](kuduClient: KuduClient,
                tableName: String,
                schema: Schema,
                createTableOptions: CreateTableOptions,
                converter: java.util.function.Function[T, PartialRow]): KuduTableSettings[T] = {

    import scala.compat.java8.FunctionConverters._

    KuduTableSettings(kuduClient, tableName, schema, createTableOptions, asScalaFromFunction(converter))
  }
}
