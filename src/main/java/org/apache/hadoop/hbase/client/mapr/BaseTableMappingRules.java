package org.apache.hadoop.hbase.client.mapr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class BaseTableMappingRules {
  public static final String HBASE_AVAILABLE = "hbase.available";

  public static final String MAPRFS_PREFIX = "maprfs://";
  public static final String HBASE_PREFIX = "hbase://";
  
  static final BaseTableMappingRules INSTANCE = new BaseTableMappingRules();

  /**
   * Describe the type of cluster based on the running services.
   */
  public enum ClusterType {
    /**
     * The cluster runs only HBase service (M3/M5)
     */
    HBASE_ONLY,
    /**
     * HBase is not installed in the cluster.
     */
    MAPR_ONLY,
    /**
     * The cluster runs both type of DB services.
     */
    HBASE_MAPR
  }

  public BaseTableMappingRules() {
  }

  /**
   * Returns one of the possible {@link ClusterType}
   * @return
   */
  public ClusterType getClusterType() {
    return ClusterType.HBASE_ONLY;
  }

  /**
   * @return <code>true</code> if Running with MapR DB and either "db.engine.default"
   * is set to "mapr" or one of the table mapping rule maps "*" to some path
   */
  public boolean isMapRDefault() {
    return false;
  }

  /**
   * @return the mapping to "*" in the namespace mapping if configured,
   * otherwise current working directory.
   */
  public Path getDefaultTablePath() {
    return null;
  }

  /**
   * Tests if <code>tableName</code> should be treated as MapR table
   *
   * @param tableName
   *
   * @return  <code>true</code> if the table is determined to be a MapR table.
   * @throws IllegalArgumentException If the passed {@code tableName} is null
   */
  public boolean isMapRTable(byte[] tableName) {
    return false;
  }

  /**
   * Tests if <code>tableName</code> should be treated as MapR table
   *
   * @param tableName
   *
   * @return  <code>true</code> if the table is determined to be a MapR table.
   * @throws IllegalArgumentException If the passed {@code tableName} is null
   */
  public boolean isMapRTable(String tableName) {
    return false;
  }

  /**
   *  Returns translated path according to the configured mapping
   *
   *  @param  tableName Absolute or relative table name
   *
   *  @return Translated absolute path if the table is a MapR table,
   *          <code>null</code> otherwise
   */
  public Path getMaprTablePath(byte[] tableName) {
    return null;
  }

  /**
   *  Returns translated path according to the configured mapping
   *
   *  @param  tableName Absolute or relative table name
   *
   *  @return Translated absolute path if the table is a MapR table,
   *          <code>null</code> otherwise
   */
  public Path getMaprTablePath(String tableName) {
    return null;
  }

  /**
   * Check if the passed , "tableName", is legal table name.
   * @return Returns passed <code>tableName</code> param
   * @throws NullPointerException If passed <code>tableName</code> is null
   * @throws IllegalArgumentException if passed a tableName is an HBase table
   * and is made of other than 'word' characters or underscores: i.e.
   * <code>[a-zA-Z_0-9].
   */
  public byte[] isLegalTableName(byte[] tableName) {
    return HTableDescriptor.isLegalTableName(tableName);
  }

  /**
   * Check if the passed , "tableName", is legal table name.
   * @return Returns passed <code>tableName</code> param
   * @throws NullPointerException If passed <code>tableName</code> is null
   * @throws IllegalArgumentException if passed a tableName is an HBase table
   * and is made of other than 'word' characters or underscores: i.e.
   * <code>[a-zA-Z_0-9].
   */
  public String isLegalTableName(String tableName) {
    return Bytes.toString(isLegalTableName(Bytes.toBytes(tableName)));
  }
}
