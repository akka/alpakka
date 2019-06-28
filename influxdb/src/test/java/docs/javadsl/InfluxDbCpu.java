/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

//#define-class
@Measurement(name = "cpu", database = "InfluxDbTest")
public class InfluxDbCpu extends Cpu {

  public InfluxDbCpu() {}

  public InfluxDbCpu(
      Instant time,
      String hostname,
      String region,
      Double idle,
      Boolean happydevop,
      Long uptimeSecs) {
    super(time, hostname, region, idle, happydevop, uptimeSecs);
  }

  public InfluxDbCpu cloneAt(Instant time) {
    return new InfluxDbCpu(
        time, getHostname(), getRegion(), getIdle(), getHappydevop(), getUptimeSecs());
  }
}
//#define-class
