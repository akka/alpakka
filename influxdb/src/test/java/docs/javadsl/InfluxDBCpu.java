/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

@Measurement(name = "cpu", database = "InfluxDBTest")
public class InfluxDBCpu extends Cpu {

  public InfluxDBCpu() {}

  public InfluxDBCpu(
      Instant time,
      String hostname,
      String region,
      Double idle,
      Boolean happydevop,
      Long uptimeSecs) {
    super(time, hostname, region, idle, happydevop, uptimeSecs);
  }

  public InfluxDBCpu cloneAt(Instant time) {
    return new InfluxDBCpu(
        time, getHostname(), getRegion(), getIdle(), getHappydevop(), getUptimeSecs());
  }
}
