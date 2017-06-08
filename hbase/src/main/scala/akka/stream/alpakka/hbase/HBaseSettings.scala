/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put

import scala.collection.immutable
import scala.reflect.ClassTag

final case class HTableSettings[T](conf: Configuration,
                                   tableName: TableName,
                                   columnFamilies: immutable.Seq[String],
                                   converter: T => immutable.Seq[Put])

object HTableSettings {

  def apply[T, X: ClassTag](conf: Configuration,
                            tableName: TableName,
                            columnFamilies: immutable.Seq[String],
                            converter: T => Put): HTableSettings[T] = {
      new HTableSettings[T](conf, tableName, columnFamilies, (t: T) => List(converter.apply(t)))
  }

  def create[T](conf: Configuration,
                tableName: TableName,
                columnFamilies: java.util.List[String],
                converter: java.util.function.Function[T, Put]): HTableSettings[T] = {
    import scala.collection.JavaConverters._
    HTableSettings(conf, tableName, immutable.Seq(columnFamilies.asScala: _*), (t: T) => List(converter.apply(t)))
  }

  def create[T](conf: Configuration,
                tableName: TableName,
                columnFamilies: java.util.List[String],
                converter: T => Put): HTableSettings[T] = {
    import scala.collection.JavaConverters._
    HTableSettings(conf, tableName, immutable.Seq(columnFamilies.asScala: _*), (t: T) => List(converter.apply(t)))
  }

  def createMulti[T](conf: Configuration,
                tableName: TableName,
                columnFamilies: java.util.List[String],
                converter: java.util.function.Function[T, java.util.List[Put]]): HTableSettings[T] = {
    import scala.collection.JavaConverters._
    HTableSettings(conf, tableName, immutable.Seq(columnFamilies.asScala: _*), (t: T) => converter.apply(t).asScala.toList)
  }

  def createMulti[T](conf: Configuration,
                tableName: TableName,
                columnFamilies: java.util.List[String],
                converter: T => immutable.Seq[Put]): HTableSettings[T] = {
    import scala.collection.JavaConverters._
    HTableSettings(conf, tableName, immutable.Seq(columnFamilies.asScala: _*), converter)
  }
}
