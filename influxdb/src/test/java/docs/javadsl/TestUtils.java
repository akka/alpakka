/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBMapper;

import static docs.javadsl.TestConstants.INFLUXDB_URL;
import static docs.javadsl.TestConstants.PASSWORD;
import static docs.javadsl.TestConstants.USERNAME;

public class TestUtils {

  public static InfluxDB setupConnection(final String databaseName) {
    final InfluxDB influxDB = InfluxDBFactory.connect(INFLUXDB_URL, USERNAME, PASSWORD);
    influxDB.setDatabase(databaseName);
    influxDB.setLogLevel(InfluxDB.LogLevel.FULL);
    influxDB.query(new Query("CREATE DATABASE " + databaseName, databaseName));
    return influxDB;
  }

  public static void populateDatabase(InfluxDB influxDB, Class<?> clazz) throws Exception {
    InfluxDBMapper influxDBMapper = new InfluxDBMapper(influxDB);
    Constructor<?> cons =
        clazz.getConstructor(
            Instant.class, String.class, String.class, Double.class, Boolean.class, Long.class);
    Object firstCore =
        cons.newInstance(
            Instant.now().minusSeconds(1000), "local_1", "eu-west-2", 1.4d, true, 123l);
    influxDBMapper.save(firstCore);
    Object secondCore =
        cons.newInstance(Instant.now().minusSeconds(500), "local_2", "eu-west-2", 1.4d, true, 123l);
    influxDBMapper.save(secondCore);
  }

  public static void cleanDatabase(final InfluxDB influxDB, final String databaseName) {
    influxDB.query(new Query("DROP MEASUREMENT cpu", databaseName));
  }

  public static void dropDatabase(InfluxDB influxDB, final String databaseName) {
    influxDB.query(new Query("DROP DATABASE " + databaseName));
  }

  public static Point resultToPoint(QueryResult.Series series, List<Object> values) {
    Point.Builder builder = Point.measurement(series.getName());

    for (int i = 0; i < series.getColumns().size(); i++) {
      String column = series.getColumns().get(i);
      Object value = values.get(i);

      if (column.equals("time")) {
        builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      } else if (column.equals("host") || column.equals("region")) {
        builder.tag(column, value.toString());
      } else if (column.equals("uptimesecs")) {
        builder.addField(column, ((Double) value).longValue());
      } else {
        if (value instanceof Long) builder.addField(column, (Long) value);
        else if (value instanceof Double) builder.addField(column, (Double) value);
        else if (value instanceof Number) builder.addField(column, (Number) value);
        else if (value instanceof String) builder.addField(column, (String) value);
        else if (value instanceof Boolean) builder.addField(column, (Boolean) value);
      }
    }

    return builder.build();
  }
}
