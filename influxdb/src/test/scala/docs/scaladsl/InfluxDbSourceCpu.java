/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

import docs.javadsl.Cpu;

@Measurement(name = "cpu", database = "InfluxDbSourceSpec")
public class InfluxDbSourceCpu extends Cpu {
    public InfluxDbSourceCpu() {
    }

    public InfluxDbSourceCpu(Instant time, String hostname, String region, Double idle, Boolean happydevop, Long uptimeSecs) {
        super(time, hostname, region, idle, happydevop, uptimeSecs);
    }

    public InfluxDbSpecCpu cloneAt(Instant time) {
        return new InfluxDbSpecCpu(time, getHostname(), getRegion(), getIdle(), getHappydevop(), getUptimeSecs());
    }
}
