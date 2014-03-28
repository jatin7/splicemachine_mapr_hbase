/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the scenario where a next() call, while scanning, timeout at client side and getting retried.
 * This scenario should not result in some data being skipped at RS side.
 */
@Category(MediumTests.class)
public class TestClientScannerRPCTimeout {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");
  private static final int SLAVES = 1;
  private static final int rpcTimeout = 5 * 1000;
  private static final int CLIENT_RETRIES_NUMBER = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, rpcTimeout);
    conf.setStrings(HConstants.REGION_SERVER_IMPL, RegionServerWithScanTimeout.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, CLIENT_RETRIES_NUMBER);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScannerNextRPCTimesout() throws Exception {
    byte[] TABLE = Bytes.toBytes("testScannerNextRPCTimesout");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    putToTable(ht, "row-1");
    putToTable(ht, "row-2");
    RegionServerWithScanTimeout.seqNoToSleepOn = 1;
    Scan scan = new Scan();
    scan.setCaching(1);
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    assertNotNull("Expected not null result", result);
    result = scanner.next();
    assertNotNull("Expected not null result", result);
    scanner.close();
    // test the case that RPC is always timesout
     scanner = ht.getScanner(scan);
     RegionServerWithScanTimeout.sleepAlways = true;
     RegionServerWithScanTimeout.tryNumber = 0;
     try {
       result = scanner.next();
     } catch (IOException ioe) {
       // catch the exception after max retry number
       LOG.info("Failed after maximal attempts=" + CLIENT_RETRIES_NUMBER, ioe);
     }
     assertTrue("Expected maximal try number=" + CLIENT_RETRIES_NUMBER
         + ", actual =" + RegionServerWithScanTimeout.tryNumber,
         RegionServerWithScanTimeout.tryNumber <= CLIENT_RETRIES_NUMBER);
  }

  private void putToTable(HTable ht, String rowkey) throws IOException {
    Put put = new Put(rowkey.getBytes());
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
  }

  private static class RegionServerWithScanTimeout extends MiniHBaseClusterRegionServer {
    private long tableScannerId;
    private boolean slept;
    private static long seqNoToSleepOn = -1;
    private static boolean sleepAlways = false;
    private static int tryNumber = 0;

    public RegionServerWithScanTimeout(Configuration conf) throws IOException,
        InterruptedException {
      super(conf);
    }

    @Override
    public long openScanner(byte[] regionName, Scan scan) throws IOException {
      long scannerId = super.openScanner(regionName, scan);
      if (!getRegionInfo(regionName).isMetaTable()) {
        tableScannerId = scannerId;
      }
      return scannerId;
    }

    @Override
    public Result[] next(long scannerId, int nbRows, long callSeq) throws IOException {
        Result[] results = super.next(scannerId, nbRows, callSeq);
      if (this.tableScannerId == scannerId &&
          (sleepAlways || (!slept && seqNoToSleepOn == callSeq))) {
        try {
          Thread.sleep(rpcTimeout + 500);
        } catch (InterruptedException e) {
        }
        slept = true;
        tryNumber++;
        if (tryNumber > 2 * CLIENT_RETRIES_NUMBER) {
          sleepAlways = false;
        }
      }
      return results;
    }
  }
}
