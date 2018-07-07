/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import akka.stream.scaladsl.Sink
import org.scalatest.{FunSuite, Matchers}

import scala.language.postfixOps

class TestScalaDsl extends FunSuite with Matchers {

  ignore("ChangeDataCaptureSettings / PostgreSQLInstance example") {

    //#ChangeDataCaptureSettings
    import scala.concurrent.duration._
    import akka.stream.alpakka.postgresqlcdc._

    val connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser"

    val postgreSQLInstance = PostgreSQLInstance(connectionString, "slot_name")

    val changeDataCaptureSettings = ChangeDataCaptureSettings()
      .withCreateSlotOnStart(false)
      .withMaxItems(256)
      .withPollInterval(7 seconds)
      .withMode(Modes.Peek)
      .withTablesToIgnore(List("user_personal_information")) // ignore the user_personal_information table
      .withColumnsToIgnore(Map("images" -> List("binary"))) // ignore the binary column in the images table
    //#ChangeDataCaptureSettings
  }

  ignore("basic example") {

    //#BasicExample

    import akka.actor.ActorSystem
    import akka.event.Logging
    import akka.stream.ActorMaterializer
    import akka.stream.Attributes
    import akka.stream.scaladsl.Sink
    import akka.stream.alpakka.postgresqlcdc._
    import akka.stream.alpakka.postgresqlcdc.scaladsl.ChangeDataCapture

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val connectionString = "jdbc:postgresql://localhost/pgdb?user=pguser&password=pguser"
    val slotName = "slot_name"

    ChangeDataCapture
      .source(PostgreSQLInstance(connectionString, slotName), ChangeDataCaptureSettings())
      .log("postgresqlcdc", cs => s"captured changes: ${cs.toString}")
      .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
      .to(Sink.ignore)
      .run()

    //#BasicExample

  }

  ignore("process events example") {

    //#ProcessEventsExample

    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.alpakka.postgresqlcdc._
    import akka.stream.alpakka.postgresqlcdc.scaladsl.ChangeDataCapture

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    // Define your domain event
    case class UserDeregistered(id: String)

    val connectionString =
      "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true"
    val slotName = "slot_name"

    ChangeDataCapture
      .source(PostgreSQLInstance(connectionString, slotName), ChangeDataCaptureSettings())
      .mapConcat(_.changes)
      .collect { // collect is map and filter
        case RowDeleted("public", "users", fields) =>
          val userId = fields.find(_.columnName == "user_id").map(_.value).getOrElse("unknown")
          UserDeregistered(userId)
      }
      .to(Sink.ignore)
      .run()

    //#ProcessEventsExample

  }

}
