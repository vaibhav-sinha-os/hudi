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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * Record Reader for ORC. Converts read values from OrcStruct to ArrayWritable.
 */
public class OrcRecordReaderWrapper implements RecordReader<NullWritable, ArrayWritable> {

  // real ORC reader to be wrapped
  private final RecordReader<NullWritable, OrcStruct> orcReader;

  private final int numValueFields;

  public OrcRecordReaderWrapper(RecordReader<NullWritable, OrcStruct> orcReader) {
    this.orcReader = orcReader;
    this.numValueFields = orcReader.createValue().getNumFields();
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    OrcStruct val = orcReader.createValue();
    boolean result = orcReader.next(key, val);
    if (result) {
      Writable[] fields = IntStream.range(0, val.getNumFields()).mapToObj(i -> (Writable) val.getFieldValue(i)).toArray(Writable[]::new);
      value.set(fields);
    }
    return result;
  }

  @Override
  public NullWritable createKey() {
    return orcReader.createKey();
  }

  @Override
  public ArrayWritable createValue() {
    Writable[] emptyWritableBuf = new Writable[numValueFields];
    return new ArrayWritable(Writable.class, emptyWritableBuf);
  }

  @Override
  public long getPos() throws IOException {
    return orcReader.getPos();
  }

  @Override
  public void close() throws IOException {
    orcReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return orcReader.getProgress();
  }
}
