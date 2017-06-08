package akka.stream.alpakka.hbase.Utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MockHTable implements HTableInterface {

    private TableName tableName;

    public MockHTable(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public byte[] getTableName() {
        return new byte[0];
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
        return 0;
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
        return new Boolean[0];
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {

    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {

    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {

    }

    @Override
    public boolean isAutoFlush() {
        return false;
    }

    @Override
    public void flushCommits() throws IOException {

    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return null;
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return false;
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        return new boolean[0];
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {

    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        return new Object[0];
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException {

    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException {
        return new Object[0];
    }

    @Override
    public Result get(Get get) throws IOException {
        return null;
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return new Result[0];
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        return null;
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        return null;
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        return null;
    }

    @Override
    public void put(Put put) throws IOException {

    }

    @Override
    public void put(List<Put> puts) throws IOException {

    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
        return false;
    }

    @Override
    public void delete(Delete delete) throws IOException {

    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {

    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
        return false;
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {

    }

    @Override
    public Result append(Append append) throws IOException {
        return null;
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        return null;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        return null;
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws Throwable {
        return null;
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws Throwable {

    }

    @Override
    public long getWriteBufferSize() {
        return 0;
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {

    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws Throwable {
        return null;
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype, Batch.Callback<R> callback) throws Throwable {

    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException {
        return false;
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        return null;
    }
}
