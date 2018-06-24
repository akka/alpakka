/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl;

import java.util.List;

public class RowInserted extends Change {

    private List<Field> fields;

    public RowInserted(String schemaName, String tableName, List<Field> fields) {
        super(schemaName, tableName);
        this.fields = fields;
    }

    public List<Field> getFields() {
        return fields;
    }

}
