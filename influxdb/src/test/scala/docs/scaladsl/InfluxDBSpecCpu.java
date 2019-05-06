/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

import docs.javadsl.Cpu;

@Measurement(name = "cpu", database = "InfluxDBSpec")
public class InfluxDBSpecCpu extends Cpu {
    public InfluxDBSpecCpu() {
    }

    public InfluxDBSpecCpu(Instant time, String hostname, String region, Double idle, Boolean happydevop, Long uptimeSecs) {
        super(time, hostname, region, idle, happydevop, uptimeSecs);
    }

    public InfluxDBSpecCpu cloneAt(Instant time) {
        return new InfluxDBSpecCpu(time, getHostname(), getRegion(), getIdle(), getHappydevop(), getUptimeSecs());
    }
}
