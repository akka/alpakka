package akka.stream.alpakka.hbase.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class MockHBaseAdmin implements Admin {

        private final HBaseMockConfiguration conf;

        public MockHBaseAdmin(HBaseMockConfiguration conf) throws IOException {
            this.conf = conf;
        }


        @Override
        public int getOperationTimeout() {
            return 0;
        }

        @Override
        public void abort(String why, Throwable e) {

        }

        @Override
        public boolean isAborted() {
            return false;
        }

        @Override
        public Connection getConnection() {
            return null;
        }

        @Override
        public boolean tableExists(TableName tableName) throws IOException {
            return false;
        }

        @Override
        public HTableDescriptor[] listTables() throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] listTables(String regex) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] listTables(Pattern pattern, boolean includeSysTables) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] listTables(String regex, boolean includeSysTables) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public TableName[] listTableNames() throws IOException {
            return new TableName[0];
        }

        @Override
        public TableName[] listTableNames(Pattern pattern) throws IOException {
            return new TableName[0];
        }

        @Override
        public TableName[] listTableNames(String regex) throws IOException {
            return new TableName[0];
        }

        @Override
        public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
            return new TableName[0];
        }

        @Override
        public TableName[] listTableNames(String regex, boolean includeSysTables) throws IOException {
            return new TableName[0];
        }

        @Override
        public HTableDescriptor getTableDescriptor(TableName tableName) throws TableNotFoundException, IOException {
            return null;
        }

        @Override
        public void createTable(HTableDescriptor desc) throws IOException {

        }

        @Override
        public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) throws IOException {

        }

        @Override
        public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {

        }

        @Override
        public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {

        }

        @Override
        public void deleteTable(TableName tableName) throws IOException {

        }

        @Override
        public HTableDescriptor[] deleteTables(String regex) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {

        }

        @Override
        public void enableTable(TableName tableName) throws IOException {

        }

        @Override
        public void enableTableAsync(TableName tableName) throws IOException {

        }

        @Override
        public HTableDescriptor[] enableTables(String regex) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public void disableTableAsync(TableName tableName) throws IOException {

        }

        @Override
        public void disableTable(TableName tableName) throws IOException {

        }

        @Override
        public HTableDescriptor[] disableTables(String regex) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public boolean isTableEnabled(TableName tableName) throws IOException {
            return false;
        }

        @Override
        public boolean isTableDisabled(TableName tableName) throws IOException {
            return false;
        }

        @Override
        public boolean isTableAvailable(TableName tableName) throws IOException {
            return conf.isTableAvailable(tableName);
        }

        @Override
        public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
            return false;
        }

        @Override
        public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
            return null;
        }

        @Override
        public void addColumn(TableName tableName, HColumnDescriptor column) throws IOException {

        }

        @Override
        public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {

        }

        @Override
        public void modifyColumn(TableName tableName, HColumnDescriptor descriptor) throws IOException {

        }

        @Override
        public void closeRegion(String regionname, String serverName) throws IOException {

        }

        @Override
        public void closeRegion(byte[] regionname, String serverName) throws IOException {

        }

        @Override
        public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName) throws IOException {
            return false;
        }

        @Override
        public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {

        }

        @Override
        public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
            return null;
        }

        @Override
        public void flush(TableName tableName) throws IOException {

        }

        @Override
        public void flushRegion(byte[] regionName) throws IOException {

        }

        @Override
        public void compact(TableName tableName) throws IOException {

        }

        @Override
        public void compactRegion(byte[] regionName) throws IOException {

        }

        @Override
        public void compact(TableName tableName, byte[] columnFamily) throws IOException {

        }

        @Override
        public void compactRegion(byte[] regionName, byte[] columnFamily) throws IOException {

        }

        @Override
        public void majorCompact(TableName tableName) throws IOException {

        }

        @Override
        public void majorCompactRegion(byte[] regionName) throws IOException {

        }

        @Override
        public void majorCompact(TableName tableName, byte[] columnFamily) throws IOException {

        }

        @Override
        public void majorCompactRegion(byte[] regionName, byte[] columnFamily) throws IOException {

        }

        @Override
        public void compactRegionServer(ServerName sn, boolean major) throws IOException, InterruptedException {

        }

        @Override
        public void move(byte[] encodedRegionName, byte[] destServerName) throws IOException {

        }

        @Override
        public void assign(byte[] regionName) throws IOException {

        }

        @Override
        public void unassign(byte[] regionName, boolean force) throws IOException {

        }

        @Override
        public void offline(byte[] regionName) throws IOException {

        }

        @Override
        public boolean setBalancerRunning(boolean on, boolean synchronous) throws IOException {
            return false;
        }

        @Override
        public boolean balancer() throws IOException {
            return false;
        }

        @Override
        public boolean isBalancerEnabled() throws IOException {
            return false;
        }

        @Override
        public boolean normalize() throws IOException {
            return false;
        }

        @Override
        public boolean isNormalizerEnabled() throws IOException {
            return false;
        }

        @Override
        public boolean setNormalizerRunning(boolean on) throws IOException {
            return false;
        }

        @Override
        public boolean enableCatalogJanitor(boolean enable) throws IOException {
            return false;
        }

        @Override
        public int runCatalogScan() throws IOException {
            return 0;
        }

        @Override
        public boolean isCatalogJanitorEnabled() throws IOException {
            return false;
        }

        @Override
        public void mergeRegions(byte[] nameOfRegionA, byte[] nameOfRegionB, boolean forcible) throws IOException {

        }

        @Override
        public void split(TableName tableName) throws IOException {

        }

        @Override
        public void splitRegion(byte[] regionName) throws IOException {

        }

        @Override
        public void split(TableName tableName, byte[] splitPoint) throws IOException {

        }

        @Override
        public void splitRegion(byte[] regionName, byte[] splitPoint) throws IOException {

        }

        @Override
        public void modifyTable(TableName tableName, HTableDescriptor htd) throws IOException {

        }

        @Override
        public void shutdown() throws IOException {

        }

        @Override
        public void stopMaster() throws IOException {

        }

        @Override
        public void stopRegionServer(String hostnamePort) throws IOException {

        }

        @Override
        public ClusterStatus getClusterStatus() throws IOException {
            return null;
        }

        @Override
        public Configuration getConfiguration() {
            return null;
        }

        @Override
        public void createNamespace(NamespaceDescriptor descriptor) throws IOException {

        }

        @Override
        public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {

        }

        @Override
        public void deleteNamespace(String name) throws IOException {

        }

        @Override
        public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
            return null;
        }

        @Override
        public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
            return new NamespaceDescriptor[0];
        }

        @Override
        public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public TableName[] listTableNamesByNamespace(String name) throws IOException {
            return new TableName[0];
        }

        @Override
        public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
            return new HTableDescriptor[0];
        }

        @Override
        public boolean abortProcedure(long procId, boolean mayInterruptIfRunning) throws IOException {
            return false;
        }

        @Override
        public ProcedureInfo[] listProcedures() throws IOException {
            return new ProcedureInfo[0];
        }

        @Override
        public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning) throws IOException {
            return null;
        }

        @Override
        public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {

        }

        @Override
        public String[] getMasterCoprocessors() throws IOException {
            return new String[0];
        }

        @Override
        public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionStateForRegion(byte[] regionName) throws IOException {
            return null;
        }

        @Override
        public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
            return 0;
        }

        @Override
        public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
            return 0;
        }

        @Override
        public void snapshot(String snapshotName, TableName tableName) throws IOException, SnapshotCreationException, IllegalArgumentException {

        }

        @Override
        public void snapshot(byte[] snapshotName, TableName tableName) throws IOException, SnapshotCreationException, IllegalArgumentException {

        }

        @Override
        public void snapshot(String snapshotName, TableName tableName, HBaseProtos.SnapshotDescription.Type type) throws IOException, SnapshotCreationException, IllegalArgumentException {

        }

        @Override
        public void snapshot(HBaseProtos.SnapshotDescription snapshot) throws IOException, SnapshotCreationException, IllegalArgumentException {

        }

        @Override
        public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshot) throws IOException, SnapshotCreationException {
            return null;
        }

        @Override
        public boolean isSnapshotFinished(HBaseProtos.SnapshotDescription snapshot) throws IOException, HBaseSnapshotException, UnknownSnapshotException {
            return false;
        }

        @Override
        public void restoreSnapshot(byte[] snapshotName) throws IOException, RestoreSnapshotException {

        }

        @Override
        public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {

        }

        @Override
        public void restoreSnapshot(byte[] snapshotName, boolean takeFailSafeSnapshot) throws IOException, RestoreSnapshotException {

        }

        @Override
        public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot) throws IOException, RestoreSnapshotException {

        }

        @Override
        public void cloneSnapshot(byte[] snapshotName, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {

        }

        @Override
        public void cloneSnapshot(String snapshotName, TableName tableName) throws IOException, TableExistsException, RestoreSnapshotException {

        }

        @Override
        public void execProcedure(String signature, String instance, Map<String, String> props) throws IOException {

        }

        @Override
        public byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props) throws IOException {
            return new byte[0];
        }

        @Override
        public boolean isProcedureFinished(String signature, String instance, Map<String, String> props) throws IOException {
            return false;
        }

        @Override
        public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
            return null;
        }

        @Override
        public List<HBaseProtos.SnapshotDescription> listSnapshots(String regex) throws IOException {
            return null;
        }

        @Override
        public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
            return null;
        }

        @Override
        public void deleteSnapshot(byte[] snapshotName) throws IOException {

        }

        @Override
        public void deleteSnapshot(String snapshotName) throws IOException {

        }

        @Override
        public void deleteSnapshots(String regex) throws IOException {

        }

        @Override
        public void deleteSnapshots(Pattern pattern) throws IOException {

        }

        @Override
        public void setQuota(QuotaSettings quota) throws IOException {

        }

        @Override
        public QuotaRetriever getQuotaRetriever(QuotaFilter filter) throws IOException {
            return null;
        }

        @Override
        public CoprocessorRpcChannel coprocessorService() {
            return null;
        }

        @Override
        public CoprocessorRpcChannel coprocessorService(ServerName sn) {
            return null;
        }

        @Override
        public void updateConfiguration(ServerName server) throws IOException {

        }

        @Override
        public void updateConfiguration() throws IOException {

        }

        @Override
        public int getMasterInfoPort() throws IOException {
            return 0;
        }

        @Override
        public List<SecurityCapability> getSecurityCapabilities() throws IOException {
            return null;
        }
    }
