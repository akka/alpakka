/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu

import org.apache.kudu.Schema
import org.apache.kudu.client.{PartialRow, _}

final case class KuduTableSettings[T](tableName: String,
                                      schema: Schema,
                                      createTableOptions: CreateTableOptions,
                                      converter: T => PartialRow)

object KuduTableSettings {
  def create[T](tableName: String,
                schema: Schema,
                createTableOptions: CreateTableOptions,
                converter: java.util.function.Function[T, PartialRow]): KuduTableSettings[T] = {

    import scala.compat.java8.FunctionConverters._

    KuduTableSettings(tableName, schema, createTableOptions, asScalaFromFunction(converter))
  }
}
