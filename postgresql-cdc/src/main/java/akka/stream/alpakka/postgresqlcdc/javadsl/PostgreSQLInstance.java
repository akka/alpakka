/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl;

public class PostgreSQLInstance {

    private String connectionString;

    private String slotName;

    private Plugin plugin;

    private int maxItems;

    private long durationMillis;

    public String getConnectionString() {
        return connectionString;
    }

    public String getSlotName() {
        return slotName;
    }

    public Plugin getPlugin() {
        return plugin;
    }

    public int getMaxItems() {
        return maxItems;
    }

    public long getDurationMillis() {
        return durationMillis;
    }


    public PostgreSQLInstance(String connectionString,
                              String slotName,
                              Plugin plugin,
                              int maxItems,
                              long durationMillis) {
        this.connectionString = connectionString;
        this.slotName = slotName;
        this.plugin = plugin;
        this.maxItems = maxItems;
        this.durationMillis = durationMillis;
    }

}