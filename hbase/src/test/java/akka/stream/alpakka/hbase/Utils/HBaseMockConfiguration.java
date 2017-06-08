package akka.stream.alpakka.hbase.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class HBaseMockConfiguration extends Configuration {

    private final MockHBaseConnection mockConnection;
    private final List<MockHTable> tables;

    public HBaseMockConfiguration(List<MockHTable> tables) throws IOException {
        this.tables = tables;
        this.mockConnection = new MockHBaseConnection(this, false, null, null);
    }

    @Override
    public Class<?> getClass(String name, Class<?> defaultValue) {
        return super.getClass(name, defaultValue);
    }

    @Override
    public String get(String name, String defaultValue) {
        if (name.equals(HConnection.HBASE_CLIENT_CONNECTION_IMPL)) {
            return MockHBaseConnection.class.getName();
        } else {
            return super.get(name, defaultValue);
        }
    }

    boolean isTableAvailable(TableName tableName) {
        for (MockHTable table : tables) {
            if (table.getName().equals(tableName))
                return true;
        }
        return false;
    }

    Table getTable(TableName tableName) {
        for (MockHTable table : tables) {
            if (table.getName().equals(tableName))
                return table;
        }
        return null;
    }

    @Override
    public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface) {
        return super.getClass(name, defaultValue, xface);
    }

    public static HBaseMockConfiguration create(List<MockHTable> tables) throws IOException {
        return new HBaseMockConfiguration(tables);
    }

    public static HBaseMockConfiguration create(MockHTable table) throws IOException {
        return new HBaseMockConfiguration(Collections.singletonList(table));
    }
}
