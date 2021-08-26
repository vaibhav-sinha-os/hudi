/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Record Reader for ORC. Converts read values from OrcStruct to ArrayWritable.
 */
public class HoodieOrcUtils {

  public static void cloneOrcStruct(OrcStruct original, OrcStruct clone) {
    for (int i = 0; i < original.getNumFields(); i++) {
      clone.setFieldValue(i, original.getFieldValue(i));
    }
    clone.setNumFields(original.getNumFields());
  }

  public static void overwriteOrcStruct(OrcStruct original, OrcStruct clone) {
    for (int i = 0; i < clone.getNumFields(); i++) {
      clone.setFieldValue(i, original.getFieldValue(i));
    }
  }

  public static OrcStruct arrayWritableToOrcStruct(ArrayWritable arrayWritable) {
    Writable[] writables = arrayWritable.get();
    OrcStruct orcStruct = new OrcStruct(writables.length);
    for (int i = 0; i < writables.length; i++) {
      orcStruct.setFieldValue(i, writables[i]);
    }
    return orcStruct;
  }

  public static void copyOrcStructToArrayWritable(OrcStruct orcStruct, ArrayWritable arrayWritable) {
    Writable[] writables = new Writable[orcStruct.getNumFields()];
    for (int i = 0; i < orcStruct.getNumFields(); i++) {
      writables[i] = (Writable) orcStruct.getFieldValue(i);
    }
    arrayWritable.set(writables);
  }

  public static ArrayWritable orcStructToArrayWritable(OrcStruct orcStruct) {
    ArrayWritable arrayWritable = new ArrayWritable(Writable.class);
    copyOrcStructToArrayWritable(orcStruct, arrayWritable);
    return arrayWritable;
  }

  public static Writable getOrcStructField(OrcStruct orcStruct, int index) {
    return (Writable) orcStruct.getFieldValue(index);
  }
}
