/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

@Measurement(name = "cpu", database = "InfluxDbSourceTest")
public class InfluxDbSourceCpu extends Cpu {

  public InfluxDbSourceCpu() {}

  public InfluxDbSourceCpu(
      Instant time,
      String hostname,
      String region,
      Double idle,
      Boolean happydevop,
      Long uptimeSecs) {
    super(time, hostname, region, idle, happydevop, uptimeSecs);
  }
}
