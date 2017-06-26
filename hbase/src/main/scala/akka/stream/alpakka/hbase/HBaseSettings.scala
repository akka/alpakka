/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.hbase

import akka.stream.alpakka.hbase.internal.HBasePutCommand
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName

import scala.collection.immutable

final case class HTableSettings[T](conf: Configuration,
                                   tableName: TableName,
                                   columnFamilies: immutable.Seq[String],
                                   converter: T => HBasePutCommand[T])
