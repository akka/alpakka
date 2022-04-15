/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import com.dimafeng.testcontainers.InfluxDBContainer;
import docs.scaladsl.InfluxDbTest;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
public class InfluxDbJUnitTest {
  public static final InfluxDBContainer CONTAINER = InfluxDbTest.createContainer();

  @BeforeClass
  public static void startContainer() {
    CONTAINER.start();
  }

  @AfterClass
  public static void stopContainer() {
    CONTAINER.stop();
  }

  public static String getInfluxUrl() {
    return CONTAINER.url();
  }

  public static final String USERNAME = InfluxDBContainer.defaultAdmin();
  public static final String PASSWORD = InfluxDBContainer.defaultAdminPassword();

  public static InfluxDB setupConnection(final String databaseName) {
    final String INFLUXDB_URL = CONTAINER.url();
    // #init-client
    final InfluxDB influxDB = InfluxDBFactory.connect(INFLUXDB_URL, USERNAME, PASSWORD);
    influxDB.setDatabase(databaseName);
    influxDB.query(new Query("CREATE DATABASE " + databaseName, databaseName));
    // #init-client
    return influxDB;
  }
}
