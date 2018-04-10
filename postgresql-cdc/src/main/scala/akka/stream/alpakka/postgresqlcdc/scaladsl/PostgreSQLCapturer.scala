/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

import akka.NotUsed
import akka.stream.alpakka.postgresqlcdc.{ChangeSet, PgSQLChangeDataCaptureSourceStage, PostgreSQLChangeDataCaptureSettings, RowDeleted}
import akka.stream.scaladsl.Source

object PostgreSQLCapturer {

  /**
    * Scala API: creates a [[PostgreSQLCapturer]]
    */
  def apply(settings: PostgreSQLChangeDataCaptureSettings): Source[ChangeSet, NotUsed] =
    Source.fromGraph(new PgSQLChangeDataCaptureSourceStage(settings))
}


object Main extends App {

  // Define your domain event
  case class UserDeregistered(id: String)

  val connectionString = "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true"
  val slotName = "slot_name"
  val settings = PostgreSQLChangeDataCaptureSettings(connectionString, slotName)

  PostgreSQLCapturer(settings)
    .map { c: ChangeSet =>
      c.changes.collect {
        case RowDeleted("public", "users", fields) => UserDeregistered(fields.find(_.columnName == "user_id").map(_.value).getOrElse("unknown"))
      }
    } // continue to a sink

}