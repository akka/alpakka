/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.postgresqlcdc.scaladsl.ChangeDataCapture
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Attributes}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps

class TestScalaDsl extends FunSuite with Matchers {

  ignore("SourceSettings snippet for docs") {

    //#SourceSettings

    val connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser"

    val postgreSQLInstance = PostgreSQLInstance(connectionString, "slot_name")

    val changeDataCaptureSettings = PgCdcSourceSettings()
      .withCreateSlotOnStart(false)
      .withMaxItems(256)
      .withPollInterval(7 seconds)
      .withMode(Modes.Peek)
      .withColumnsToIgnore(Map("images" -> List("binary"), "user_personal_information" -> List("*")))
    // ignore the binary column in the images table and ignore the user_personal_information table
    //#SourceSettings
  }

  ignore("AckSinkSettings snippet for docs") {

    //#AckSinkSettings

    val connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser"

    val postgreSQLInstance = PostgreSQLInstance(connectionString, "slot_name")

    val ackSinkSettings = PgCdcAckSinkSettings()
      .withMaxItems(16)
      .withMaxItemsWait(7 seconds)

    //#AckSinkSettings

  }

  ignore("Source usage (get mode) snippet for docs") {

    //#GetExample

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val connectionString = "jdbc:postgresql://localhost/pgdb?user=pguser&password=pguser"
    val slotName = "slot_name"

    ChangeDataCapture
      .source(PostgreSQLInstance(connectionString, slotName), PgCdcSourceSettings())
      .log("postgresqlcdc", cs => s"captured changes: ${cs.toString}")
      .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
      .to(Sink.ignore)
      .run()

    //#GetExample

  }

  ignore("Source usage (peek mode) snippet for docs") {

    //#PeekExample

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    // Define your domain event
    case class UserRegistered(id: String)

    val connectionString =
      "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true"
    val slotName = "slot_name"

    val pgSqlInstance = PostgreSQLInstance(connectionString, slotName)
    val source = ChangeDataCapture.source(pgSqlInstance, PgCdcSourceSettings())
    val ackSink = ChangeDataCapture.ackSink(pgSqlInstance, PgCdcAckSinkSettings())

    source
      .collect { // collect is map and filter
        case cs @ ChangeSet(_, _, _, RowInserted("public", "users", fields) :: Nil) =>
          val userId = fields.find(_.columnName == "user_id").map(_.value).getOrElse("unknown")
          (cs, UserRegistered(userId))
      }
      .map(identity) // do something useful e.g., publish to SQS
      .map(_._1)
      .to(ackSink) // acknowledge
      .run()

    //#PeekExample

  }

}
