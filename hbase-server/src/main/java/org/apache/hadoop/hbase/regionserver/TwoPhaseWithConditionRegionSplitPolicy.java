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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;


public class TwoPhaseWithConditionRegionSplitPolicy
extends ConstantSizeRegionSplitPolicy {
  static final Log LOG =
    LogFactory.getLog(TwoPhaseWithConditionRegionSplitPolicy.class);
  
  public final static String HBASE_TWOPHASE_POLICY_CONDITION_CLASS = 
      "hbase.twophase.policy.condition.class";
  
  public static interface SecondPhaseCondition{
    
    public void configureForRegion(HRegion region);
    public boolean isConditionMet();
    
  }
  
  private static class DefaultSecondPhaseCondition implements SecondPhaseCondition
  {

    @Override
    public void configureForRegion(HRegion region) {
      // Does nothing
    }

    @Override
    public boolean isConditionMet() {
      // Always return true
      return true;
    }    
  }
  
  private long initialSize;

  SecondPhaseCondition condition;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    Configuration conf = getConf();
    
    this.initialSize = conf.getLong(HConstants.HBASE_TWOPHASE_POLICY_INITIAL_SIZE, -1);
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
    
    initializeSecondPhaseCondition(conf);
    
  }


  private void initializeSecondPhaseCondition(Configuration conf) {
    String className = conf.get(HBASE_TWOPHASE_POLICY_CONDITION_CLASS);
    if( className == null){
      condition = new DefaultSecondPhaseCondition();
    } else{
      try {
        Class<?> cls = Class.forName(className);
        condition = (SecondPhaseCondition) cls.newInstance();
        condition.configureForRegion(region);
      } catch (Exception e) {
        condition = new DefaultSecondPhaseCondition();
        LOG.error("[TwoPhaseRegionSplitPolicy] Second phase condition class is not defined", e);
      }
    }
  }


  @Override
  protected boolean shouldSplit() {

    if (region.shouldForceSplit()) return true;
    boolean foundABigStore = false;

    // Get size to check
    long sizeToCheck = getSizeToCheck();

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
          " size=" + size + ", sizeToCheck=" + sizeToCheck );
        foundABigStore = true;
        break;
      }
    }

    return foundABigStore;
  }


/**
   * @return Region max size or this policy initial size. 
   */
  protected long getSizeToCheck() {
    return  (condition.isConditionMet()) ? getDesiredMaxFileSize(): this.initialSize;      
  }


}
