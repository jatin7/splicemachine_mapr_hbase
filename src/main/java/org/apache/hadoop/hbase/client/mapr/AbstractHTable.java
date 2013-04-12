package org.apache.hadoop.hbase.client.mapr;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This class defines public methods implemented in {@link HTable} class
 */
public abstract class AbstractHTable {

  public abstract void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException;

  public abstract Object[] batch(List<? extends Row> actions)
      throws IOException, InterruptedException;

  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  public HRegionLocation getRegionLocation(final String row) throws IOException {
    return getRegionLocation(Bytes.toBytes(row));
  }

  public abstract HRegionLocation getRegionLocation(final byte[] row)
      throws IOException;

  public HRegionLocation getRegionLocation(byte[] row, boolean reload)
      throws IOException {
    return getRegionLocation(row);
  }

  public byte[][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  public abstract Pair<byte[][],byte[][]> getStartEndKeys() throws IOException;

  public abstract NavigableMap<HRegionInfo, ServerName> getRegionLocations()
      throws IOException;

  public long getWriteBufferSize() {
    return 0;
  }

  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    /* NO-OP */
  }

  public RowLock lockRow(byte[] row) throws IOException {
    return null;
  }

  public void unlockRow(RowLock rl) throws IOException {
  }

  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol,
      byte[] row) {
    return null;
  }

  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey, Call<T, R> callable)
      throws IOException, Throwable {
    return null;
  }

  public <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey, Call<T, R> callable,
      Callback<R> callback) throws IOException, Throwable {
  }

  public void clearRegionCache() {
  }

  public int getOperationTimeout() {
    return 0;
  }

  public void setOperationTimeout(int operationTimeout) {
  }

  public abstract byte[] getTableName();

  public abstract Configuration getConfiguration();

  public abstract HTableDescriptor getTableDescriptor() throws IOException;

  public abstract boolean exists(Get get) throws IOException;

  public abstract Result get(Get get) throws IOException;

  public abstract Result[] get(List<Get> gets) throws IOException;

  public abstract Result getRowOrBefore(byte[] row, byte[] family)
      throws IOException;

  public abstract ResultScanner getScanner(Scan scan) throws IOException;

  public abstract void put(Put put) throws IOException;

  public abstract void put(List<Put> puts) throws IOException;

  public abstract boolean checkAndPut(byte[] row, byte[] family, 
      byte[] qualifier, byte[] value, Put put) throws IOException;

  public abstract void delete(Delete delete) throws IOException;

  public abstract void delete(List<Delete> deletes) throws IOException;

  public abstract boolean checkAndDelete(byte[] row, byte[] family,
      byte[] qualifier, byte[] value, Delete delete) throws IOException;

  public abstract void mutateRow(RowMutations rm) throws IOException;

  public abstract Result append(Append append) throws IOException;

  public abstract Result increment(Increment increment) throws IOException;

  public abstract long incrementColumnValue(byte[] row, byte[] family,
      byte[] qualifier, long amount) throws IOException;

  public abstract long incrementColumnValue(byte[] row, byte[] family,
      byte[] qualifier, long amount, boolean writeToWAL) throws IOException;

  public abstract boolean isAutoFlush();

  public abstract void flushCommits() throws IOException;

  public abstract void close() throws IOException;

  public abstract void setAutoFlush(boolean autoFlush);

  public abstract void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail);

}
