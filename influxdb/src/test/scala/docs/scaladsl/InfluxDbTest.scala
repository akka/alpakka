/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.testkit.TestKitBase
import com.dimafeng.testcontainers.{ForAllTestContainer, InfluxDBContainer}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.influxdb.dto.Query
import org.scalatest.{BeforeAndAfterAll, Suite}

trait InfluxDbTest extends ForAllTestContainer with BeforeAndAfterAll with TestKitBase { self: Suite =>
  override lazy val container = new InfluxDBContainer()

  lazy val InfluxDbUrl: String = container.url
  lazy val UserName: String = InfluxDBContainer.defaultAdmin
  lazy val Password: String = InfluxDBContainer.defaultAdminPassword

  def setupConnection(databaseName: String): InfluxDB = {
    //#init-client
    val influxDB = InfluxDBFactory.connect(InfluxDbUrl, UserName, Password)
    influxDB.setDatabase(databaseName)
    influxDB.query(new Query("CREATE DATABASE " + databaseName, databaseName))
    //#init-client
    influxDB
  }

  override protected def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}

object InfluxDbTest {
  def createContainer: InfluxDBContainer = new InfluxDBContainer()
}
