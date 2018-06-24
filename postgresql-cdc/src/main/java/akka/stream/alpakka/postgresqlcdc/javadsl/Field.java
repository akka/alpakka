/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl;

public class Field {

    private String columnName;

    private String columnType;

    private String value;

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public String getValue() {
        return value;
    }

    public Field(String columnName, String columnType, String value) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.value = value;
    }

}
