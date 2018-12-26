/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Mutation

import scala.collection.immutable

final case class HTableSettings[T](conf: Configuration,
                                   tableName: TableName,
                                   columnFamilies: immutable.Seq[String],
                                   converter: T => immutable.Seq[Mutation])

object HTableSettings {

  def create[T](conf: Configuration,
                tableName: TableName,
                columnFamilies: java.util.List[String],
                converter: java.util.function.Function[T, java.util.List[Mutation]]): HTableSettings[T] = {
    import scala.compat.java8.FunctionConverters._
    import scala.collection.JavaConverters._

    val mutationsFunc = (m: T) => {
      val list = asScalaFromFunction(converter)(m)

      immutable.Seq(list.asScala: _*)
    }

    HTableSettings(conf, tableName, immutable.Seq(columnFamilies.asScala: _*), mutationsFunc)
  }
}
