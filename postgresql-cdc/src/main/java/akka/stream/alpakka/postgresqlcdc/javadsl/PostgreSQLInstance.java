/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl;

public final class PostgreSQLInstance {

    private String connectionString;

    private String slotName;

    private long peekFrom;

    private int maxItems;

    private long durationMillis;

    public String getConnectionString() {
        return connectionString;
    }

    public String getSlotName() {
        return slotName;
    }

    public long getPeekFrom() {
        return peekFrom;
    }

    public int getMaxItems() {
        return maxItems;
    }

    public long getDurationMillis() {
        return durationMillis;
    }


    public PostgreSQLInstance(String connectionString,
                              String slotName,
                              long peekFrom,
                              int maxItems,
                              long durationMillis) {
        this.connectionString = connectionString;
        this.slotName = slotName;
        this.peekFrom = peekFrom;
        this.maxItems = maxItems;
        this.durationMillis = durationMillis;
    }

}