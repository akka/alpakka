package akka.stream.alpakka.hbase.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class MockHBaseConnection implements Connection {

        private final HBaseMockConfiguration conf;
        private final MockHBaseAdmin admin;

        public MockHBaseConnection(Configuration conf, final boolean managed, final ExecutorService pool, final User user) throws IOException {
            this.conf = (HBaseMockConfiguration) conf;
            this.admin = new MockHBaseAdmin(this.conf);
        }

        @Override
        public Configuration getConfiguration() {
            return conf;
        }

        @Override
        public Table getTable(TableName tableName) throws IOException {
            return conf.getTable(tableName);
        }

        @Override
        public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
            return null;
        }

        @Override
        public RegionLocator getRegionLocator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Admin getAdmin() throws IOException {
            return admin;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void abort(String why, Throwable e) {

        }

        @Override
        public boolean isAborted() {
            return false;
        }
    }
