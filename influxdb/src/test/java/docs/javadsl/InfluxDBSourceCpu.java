/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

@Measurement(name = "cpu", database = "InfluxDBSourceTest")
public class InfluxDBSourceCpu extends Cpu {

  public InfluxDBSourceCpu() {}

  public InfluxDBSourceCpu(
      Instant time,
      String hostname,
      String region,
      Double idle,
      Boolean happydevop,
      Long uptimeSecs) {
    super(time, hostname, region, idle, happydevop, uptimeSecs);
  }
}
