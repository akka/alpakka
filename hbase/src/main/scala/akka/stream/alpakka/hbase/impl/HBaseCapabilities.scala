/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hbase.impl

import java.io.Closeable

import akka.stream.stage.StageLogging
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

import scala.concurrent.ExecutionContext.Implicits.global

import scala.language.postfixOps

private[impl] trait HBaseCapabilities { this: StageLogging =>

  def twr[A <: Closeable, B](resource: A)(doWork: A => B): Try[B] =
    try {
      Success(doWork(resource))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      try {
        if (resource != null) {
          resource.close()
        }
      } catch {
        case e: Exception => log.error(e, e.getMessage) // should be logged
      }
    }

  /**
   * Connect to hbase cluster.
   *
   * @param conf
   * @param timeout in second
   * @return
   */
  def connect(conf: Configuration, timeout: Int = 10) =
    Await.result(Future(ConnectionFactory.createConnection(conf)), timeout seconds)

  private[impl] def getOrCreateTable(tableName: TableName, columnFamilies: Seq[String])(implicit
      connection: Connection
  ): Try[Table] = twr(connection.getAdmin) { admin =>
    val table =
      if (admin.isTableAvailable(tableName))
        connection.getTable(tableName)
      else {
        val tableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)
        columnFamilies.foreach { cf =>
          tableDescriptor.addFamily(new HColumnDescriptor(cf))
        }
        admin.createTable(tableDescriptor)
        log.info(s"Table $tableName created with cfs: $columnFamilies.")
        connection.getTable(tableName)
      }
    table
  }

}
