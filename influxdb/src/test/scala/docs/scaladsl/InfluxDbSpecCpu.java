/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

import docs.javadsl.Cpu;

//#define-class
@Measurement(name = "cpu", database = "InfluxDbSpec")
public class InfluxDbSpecCpu extends Cpu {
    public InfluxDbSpecCpu() {
    }

    public InfluxDbSpecCpu(Instant time, String hostname, String region, Double idle, Boolean happydevop, Long uptimeSecs) {
        super(time, hostname, region, idle, happydevop, uptimeSecs);
    }

    public InfluxDbSpecCpu cloneAt(Instant time) {
        return new InfluxDbSpecCpu(time, getHostname(), getRegion(), getIdle(), getHappydevop(), getUptimeSecs());
    }
}
//#define-class