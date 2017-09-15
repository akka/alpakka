/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ibm.eventstore.scaladsl

import akka.Done
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.ibm.event.oltp.EventContext
import com.ibm.event.oltp.InsertResult
import org.apache.spark.sql.Row

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object EventStoreSink {

  def apply(
      databaseName: String,
      tableName: String,
      parallelism: Int = 1
  )(implicit ec: ExecutionContext): Sink[Row, Future[Done]] = {

    val context = EventContext.getEventContext(databaseName)
    val schema = context.getTable(tableName)

    Flow[Row]
      .mapAsyncUnordered(parallelism) { row ⇒
        context.insertAsync(schema, row)
      }
      .toMat(Sink.ignore)(Keep.right)
  }
}

object EventStoreFlow {
  def apply(
      databaseName: String,
      tableName: String,
      parallelism: Int = 1
  )(implicit ec: ExecutionContext): Flow[Row, InsertResult, NotUsed] = {

    val context = EventContext.getEventContext(databaseName)
    val schema = context.getTable(tableName)

    Flow[Row]
      .mapAsyncUnordered(parallelism) { row ⇒
        context.insertAsync(schema, row)
      }
  }
}
