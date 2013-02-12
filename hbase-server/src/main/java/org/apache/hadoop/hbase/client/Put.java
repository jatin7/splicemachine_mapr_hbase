/*
 *
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Used to perform Put operations for a single row.
 * <p>
 * To perform a Put, instantiate a Put object with the row to insert to and
 * for each column to be inserted, execute {@link #add(byte[], byte[], byte[]) add} or
 * {@link #add(byte[], byte[], long, byte[]) add} if setting the timestamp.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Put extends Mutation implements HeapSize, Comparable<Row> {
  private static final long OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + 2 * ClassSize.REFERENCE +
      1 * Bytes.SIZEOF_LONG + Bytes.SIZEOF_BOOLEAN +
      ClassSize.REFERENCE + ClassSize.TREEMAP);

  /**
   * Create a Put operation for the specified row.
   * @param row row key
   */
  public Put(byte [] row) {
    this(row, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Create a Put operation for the specified row, using a given timestamp.
   *
   * @param row row key
   * @param ts timestamp
   */
  public Put(byte[] row, long ts) {
    if(row == null || row.length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("Row key is invalid");
    }
    this.row = Arrays.copyOf(row, row.length);
    this.ts = ts;
  }

  /**
   * Copy constructor.  Creates a Put operation cloned from the specified Put.
   * @param putToCopy put to copy
   */
  public Put(Put putToCopy) {
    this(putToCopy.getRow(), putToCopy.ts);
    this.familyMap =
      new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte [], List<KeyValue>> entry :
      putToCopy.getFamilyMap().entrySet()) {
      this.familyMap.put(entry.getKey(), entry.getValue());
    }
    this.writeToWAL = putToCopy.writeToWAL;
  }

  /**
   * Add the specified column and value to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value column value
   * @return this
   */
  public Put add(byte [] family, byte [] qualifier, byte [] value) {
    return add(family, qualifier, this.ts, value);
  }

  /**
   * Add the specified column and value, with the specified timestamp as
   * its version to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param ts version timestamp
   * @param value column value
   * @return this
   */
  public Put add(byte [] family, byte [] qualifier, long ts, byte [] value) {
    List<KeyValue> list = getKeyValueList(family);
    KeyValue kv = createPutKeyValue(family, qualifier, ts, value);
    list.add(kv);
    familyMap.put(kv.getFamily(), list);
    return this;
  }

  /**
   * Add the specified KeyValue to this Put operation.  Operation assumes that
   * the passed KeyValue is immutable and its backing array will not be modified
   * for the duration of this Put.
   * @param kv individual KeyValue
   * @return this
   * @throws java.io.IOException e
   */
  public Put add(KeyValue kv) throws IOException{
    byte [] family = kv.getFamily();
    List<KeyValue> list = getKeyValueList(family);
    //Checking that the row of the kv is the same as the put
    int res = Bytes.compareTo(this.row, 0, row.length,
        kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
    if(res != 0) {
      throw new IOException("The row in the recently added KeyValue " +
          Bytes.toStringBinary(kv.getBuffer(), kv.getRowOffset(),
        kv.getRowLength()) + " doesn't match the original one " +
        Bytes.toStringBinary(this.row));
    }
    list.add(kv);
    familyMap.put(family, list);
    return this;
  }

  /*
   * Create a KeyValue with this objects row key and the Put identifier.
   *
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  private KeyValue createPutKeyValue(byte[] family, byte[] qualifier, long ts,
      byte[] value) {
  return  new KeyValue(this.row, family, qualifier, ts, KeyValue.Type.Put,
      value);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * a value assigned to the given family & qualifier.
   * Both given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @return returns true if the given family and qualifier already has an
   * existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier) {
  return has(family, qualifier, this.ts, new byte[0], true, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * a value assigned to the given family, qualifier and timestamp.
   * All 3 given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @param ts timestamp
   * @return returns true if the given family, qualifier and timestamp already has an
   * existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier, long ts) {
  return has(family, qualifier, ts, new byte[0], false, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * a value assigned to the given family, qualifier and timestamp.
   * All 3 given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @param value value to check
   * @return returns true if the given family, qualifier and value already has an
   * existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier, byte [] value) {
    return has(family, qualifier, this.ts, value, true, false);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * the given value assigned to the given family, qualifier and timestamp.
   * All 4 given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @param ts timestamp
   * @param value value to check
   * @return returns true if the given family, qualifier timestamp and value
   * already has an existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier, long ts, byte [] value) {
      return has(family, qualifier, ts, value, false, false);
  }

  /*
   * Private method to determine if this object's familyMap contains
   * the given value assigned to the given family, qualifier and timestamp
   * respecting the 2 boolean arguments
   *
   * @param family
   * @param qualifier
   * @param ts
   * @param value
   * @param ignoreTS
   * @param ignoreValue
   * @return returns true if the given family, qualifier timestamp and value
   * already has an existing KeyValue object in the family map.
   */
  private boolean has(byte[] family, byte[] qualifier, long ts, byte[] value,
                      boolean ignoreTS, boolean ignoreValue) {
    List<KeyValue> list = getKeyValueList(family);
    if (list.size() == 0) {
      return false;
    }
    // Boolean analysis of ignoreTS/ignoreValue.
    // T T => 2
    // T F => 3 (first is always true)
    // F T => 2
    // F F => 1
    if (!ignoreTS && !ignoreValue) {
      for (KeyValue kv : list) {
        if (Arrays.equals(kv.getFamily(), family) &&
            Arrays.equals(kv.getQualifier(), qualifier) &&
            Arrays.equals(kv.getValue(), value) &&
            kv.getTimestamp() == ts) {
          return true;
        }
      }
    } else if (ignoreValue && !ignoreTS) {
      for (KeyValue kv : list) {
        if (Arrays.equals(kv.getFamily(), family) && Arrays.equals(kv.getQualifier(), qualifier)
            && kv.getTimestamp() == ts) {
          return true;
        }
      }
    } else if (!ignoreValue && ignoreTS) {
      for (KeyValue kv : list) {
        if (Arrays.equals(kv.getFamily(), family) && Arrays.equals(kv.getQualifier(), qualifier)
            && Arrays.equals(kv.getValue(), value)) {
          return true;
        }
      }
    } else {
      for (KeyValue kv : list) {
        if (Arrays.equals(kv.getFamily(), family) &&
            Arrays.equals(kv.getQualifier(), qualifier)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns a list of all KeyValue objects with matching column family and qualifier.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @return a list of KeyValue objects with the matching family and qualifier,
   * returns an empty list if one doesnt exist for the given family.
   */
  public List<KeyValue> get(byte[] family, byte[] qualifier) {
    List<KeyValue> filteredList = new ArrayList<KeyValue>();
    for (KeyValue kv: getKeyValueList(family)) {
      if (Arrays.equals(kv.getQualifier(), qualifier)) {
        filteredList.add(kv);
      }
    }
    return filteredList;
  }

  /**
   * Creates an empty list if one doesnt exist for the given column family
   * or else it returns the associated list of KeyValue objects.
   *
   * @param family column family
   * @return a list of KeyValue objects, returns an empty list if one doesnt exist.
   */
  private List<KeyValue> getKeyValueList(byte[] family) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>(0);
    }
    return list;
  }

  //HeapSize
  public long heapSize() {
    long heapsize = OVERHEAD;
    //Adding row
    heapsize += ClassSize.align(ClassSize.ARRAY + this.row.length);

    //Adding map overhead
    heapsize +=
      ClassSize.align(this.familyMap.size() * ClassSize.MAP_ENTRY);
    for(Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      //Adding key overhead
      heapsize +=
        ClassSize.align(ClassSize.ARRAY + entry.getKey().length);

      //This part is kinds tricky since the JVM can reuse references if you
      //store the same value, but have a good match with SizeOf at the moment
      //Adding value overhead
      heapsize += ClassSize.align(ClassSize.ARRAYLIST);
      int size = entry.getValue().size();
      heapsize += ClassSize.align(ClassSize.ARRAY +
          size * ClassSize.REFERENCE);

      for(KeyValue kv : entry.getValue()) {
        heapsize += kv.heapSize();
      }
    }
    heapsize += getAttributeSize();

    return ClassSize.align((int)heapsize);
  }
}