/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import static docs.javadsl.TestConstants.DATABASE_NAME;

@Measurement(
    name = "cpu",
    database = DATABASE_NAME,
    retentionPolicy = "autogen",
    timeUnit = TimeUnit.MILLISECONDS)
public class Cpu {

  @Column(name = "time")
  private Instant time;

  @Column(name = "host", tag = true)
  private String hostname;

  @Column(name = "region", tag = true)
  private String region;

  @Column(name = "idle")
  private Double idle;

  @Column(name = "happydevop")
  private Boolean happydevop;

  @Column(name = "uptimesecs")
  private Long uptimeSecs;

  public Cpu() {}

  public Cpu(
      Instant time,
      String hostname,
      String region,
      Double idle,
      Boolean happydevop,
      Long uptimeSecs) {
    this.time = time;
    this.hostname = hostname;
    this.region = region;
    this.idle = idle;
    this.happydevop = happydevop;
    this.uptimeSecs = uptimeSecs;
  }

  public Instant getTime() {
    return time;
  }

  public String getHostname() {
    return hostname;
  }

  public String getRegion() {
    return region;
  }

  public Double getIdle() {
    return idle;
  }

  public Boolean getHappydevop() {
    return happydevop;
  }

  public Long getUptimeSecs() {
    return uptimeSecs;
  }

  public Cpu cloneAt(Instant time) {
    return new Cpu(time, hostname, region, idle, happydevop, uptimeSecs);
  }
}
