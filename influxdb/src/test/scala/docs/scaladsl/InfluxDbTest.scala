/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import com.dimafeng.testcontainers.{ForAllTestContainer, InfluxDBContainer}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.influxdb.dto.Query
import org.scalatest.Suite

trait InfluxDbTest extends ForAllTestContainer { self: Suite =>
  override lazy val container = new InfluxDBContainer()

  lazy val InfluxDbUrl: String = container.url
  lazy val UserName: String = InfluxDBContainer.defaultAdmin
  lazy val Password: String = InfluxDBContainer.defaultAdminPassword

  def setupConnection(databaseName: String): InfluxDB = {
    val influxDB = InfluxDBFactory.connect(InfluxDbUrl, UserName, Password)
    influxDB.setDatabase(databaseName)
    influxDB.query(new Query("CREATE DATABASE " + databaseName, databaseName))
    influxDB
  }
}

object InfluxDbTest {
  def createContainer: InfluxDBContainer = new InfluxDBContainer()
}
