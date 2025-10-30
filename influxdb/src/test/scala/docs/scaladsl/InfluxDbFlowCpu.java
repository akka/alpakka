/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl;

import java.time.Instant;

import org.influxdb.annotation.Measurement;

import docs.javadsl.Cpu;

@Measurement(name = "cpu", database = "FlowSpec")
public class InfluxDbFlowCpu extends Cpu {

    public InfluxDbFlowCpu() {
    }

    public InfluxDbFlowCpu(Instant time, String hostname, String region, Double idle, Boolean happydevop, Long uptimeSecs) {
        super(time, hostname, region, idle, happydevop, uptimeSecs);
    }

    public InfluxDbFlowCpu cloneAt(Instant time) {
        return new InfluxDbFlowCpu(time, getHostname(), getRegion(), getIdle(), getHappydevop(), getUptimeSecs());
    }

}

