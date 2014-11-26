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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Split size depends on a total number of online regions for this table
 * (cluster-wide). If this number less than predefined threshold, defined by
 * hbase.doublephase.policy.threshold ( default is double of cluster size) 
 * than it is defined by hbase.doublephase.policy.initial.size configuration 
 * value (default, is MEMSTORE_FLUSH_SIZE), otherwise it equals to
 * HREGION_MAX_FILESIZE
 *  
 */
public class DoublePhaseRegionSplitPolicy
extends ConstantSizeRegionSplitPolicy {
  static final Log LOG =
    LogFactory.getLog(DoublePhaseRegionSplitPolicy.class);
  private long initialSize;
  private int  regionsThreshold;
  private boolean secondTryDone = false;
  // TODO: is it safe to cache HBaseAdmin?
  HBaseAdmin admin;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    Configuration conf = getConf();
    this.initialSize = conf.getLong(HConstants.HBASE_DOUBLEPHASE_POLICY_INITIAL_SIZE, -1);
    if (this.initialSize < 0) {
      HTableDescriptor desc = region.getTableDesc();
      if (desc != null) {
        this.initialSize = desc.getMemStoreFlushSize();
      }
      if (this.initialSize <= 0) {
        this.initialSize =
            conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
              HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE);
      }
    }
    
    this.regionsThreshold = conf.getInt(HConstants.HBASE_DOUBLEPHASE_POLICY_THRESHOLD, -1);
    if(this.regionsThreshold < 0){
      // get the cluster size
      initDefaultRegionsThreshold(); 
    }
  }

  private void initDefaultRegionsThreshold() {
    try {
      if( this.admin == null){
        this.admin = new HBaseAdmin(region.getConf());
      }
      ClusterStatus status = this.admin.getClusterStatus();
      this.regionsThreshold = 2 * status.getServersSize();
      // It is possible that we do not have the full list of servers yet
      if( this.regionsThreshold < 2){
        this.regionsThreshold = 2;
      }
      
    } catch (Exception e) {
      LOG.error("Could not get the cluster size", e);
      //Let us play safe
      this.regionsThreshold = 2;
    } 
    
  }

  @Override
  protected boolean shouldSplit() {
    // Try one more time
    checkDefaultRegionsThreshold();
    if (region.shouldForceSplit()) return true;
    boolean foundABigStore = false;
    // Get count of regions that have the same common table as this.region
    int tableRegionsCount = getGlobalCountOfTableRegions();
    // Get size to check
    long sizeToCheck = getSizeToCheck(tableRegionsCount);

    for (Store store : region.getStores().values()) {
      // If any of the stores is unable to split (e.g. they contain reference files)
      // then don't split
      if ((!store.canSplit())) {
        return false;
      }

      // Mark if any store is big enough
      long size = store.getSize();
      if (size > sizeToCheck) {
        LOG.debug("ShouldSplit because " + store.getColumnFamilyName() +
          " size=" + size + ", sizeToCheck=" + sizeToCheck +
          ", regionsWithCommonTable=" + tableRegionsCount);
        foundABigStore = true;
        break;
      }
    }

    return foundABigStore;
  }

  private void checkDefaultRegionsThreshold() {
    if( secondTryDone == true) return;
    secondTryDone = true;
    if( region.getConf().getInt(HConstants.HBASE_DOUBLEPHASE_POLICY_THRESHOLD, -1) > 0)
    {
      return;
    }
    initDefaultRegionsThreshold();
  }

  @Override
  protected boolean isSplitRecommended() {
	    if (region.shouldForceSplit()) return true;
	    boolean foundABigStore = false;
	    // Get count of regions that have the same common table as this.region
	    int tableRegionsCount = getGlobalCountOfTableRegions();
	    // Get size to check
	    long sizeToCheck = getSizeToCheck(tableRegionsCount);

	    for (Store store : region.getStores().values()) {

	      // Mark if any store is big enough
	      long size = store.getSize();
	      if (size > sizeToCheck) {
	        LOG.debug("ShouldSplit because " + store.getColumnFamilyName() +
	          " size=" + size + ", sizeToCheck=" + sizeToCheck +
	          ", gloabalRegionsTable=" + tableRegionsCount);
	        foundABigStore = true;
	        break;
	      }
	    }

	    return foundABigStore;
  }

/**
   * @return Region max size or this policy initial size. 
   */
  protected long getSizeToCheck(final int tableRegionsCount) {
    return tableRegionsCount == 0 || tableRegionsCount > 
      this.regionsThreshold ? getDesiredMaxFileSize(): this.initialSize;      
  }

  /**
   * @return Total number of regions across the cluster (for this table)
   * 
   */
  private int getGlobalCountOfTableRegions() {

    byte [] tablename = this.region.getTableDesc().getName();
    int tableRegionsCount = 0;
    try {
      List<HRegionInfo> regionList = this.admin.getTableRegions(tablename);
      tableRegionsCount = (regionList == null)? 0: regionList.size();
    } catch (IOException e) {
      LOG.debug("Failed getOnlineRegions " + Bytes.toString(tablename), e);
    }
    return tableRegionsCount;
  }
}
