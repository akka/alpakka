/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import org.scalatest.{FunSuite, Matchers}

class TestScalaDsl extends FunSuite with Matchers {

  test("ChangeDataCaptureSettings / PostgreSQLInstance can be constructed properly") {

    //#ChangeDataCaptureSettings
    import scala.concurrent.duration._

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

}
