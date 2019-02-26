/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Mutation

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

final class HTableSettings[T] private (val conf: Configuration,
                                       val tableName: TableName,
                                       val columnFamilies: immutable.Seq[String],
                                       val converter: T => immutable.Seq[Mutation]) {

  def withConf(conf: Configuration): HTableSettings[T] =
    copy(conf = conf)

  def withTableName(tableName: TableName): HTableSettings[T] =
    copy(tableName = tableName)

  def withColumnFamilies(columnFamilies: immutable.Seq[String]): HTableSettings[T] =
    copy(columnFamilies = columnFamilies)

  /**
   * Java Api
   */
  def withColumnFamilies(columnFamilies: java.util.List[String]): HTableSettings[T] =
    copy(columnFamilies = columnFamilies.asScala.toIndexedSeq)

  def withConverter(converter: T => immutable.Seq[Mutation]): HTableSettings[T] =
    copy(converter = converter)

  /**
   * Java Api
   */
  def withConverter(converter: java.util.function.Function[T, java.util.List[Mutation]]): HTableSettings[T] =
    copy(converter = converter.asScala(_).asScala.toIndexedSeq)

  override def toString: String =
    "HTableSettings(" +
    s"conf=$conf," +
    s"tableName=$tableName," +
    s"columnFamilies=$columnFamilies" +
    s"converter=$converter" +
    ")"

  private def copy(conf: Configuration = conf,
                   tableName: TableName = tableName,
                   columnFamilies: immutable.Seq[String] = columnFamilies,
                   converter: T => immutable.Seq[Mutation] = converter) =
    new HTableSettings[T](conf, tableName, columnFamilies, converter)

}

object HTableSettings {

  /**
   * Create table settings, describing table name, columns and HBase mutations for every model object
   */
  def apply[T](conf: Configuration,
               tableName: TableName,
               columnFamilies: immutable.Seq[String],
               converter: T => immutable.Seq[Mutation]) =
    new HTableSettings(conf, tableName, columnFamilies, converter)

  /**
   * Java Api
   * Create table settings, describing table name, columns and HBase mutations for every model object
   */
  def create[T](conf: Configuration,
                tableName: TableName,
                columnFamilies: java.util.List[String],
                converter: java.util.function.Function[T, java.util.List[Mutation]]): HTableSettings[T] =
    HTableSettings(conf, tableName, columnFamilies.asScala.toIndexedSeq, converter.asScala(_).asScala.toIndexedSeq)
}
