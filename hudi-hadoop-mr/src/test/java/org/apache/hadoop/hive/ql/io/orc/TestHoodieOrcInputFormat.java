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

import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestHoodieOrcInputFormat {

  private HoodieOrcInputFormat inputFormat;

  @Test
  public void testStaticMethod() throws IOException {
    inputFormat = new HoodieOrcInputFormat();
    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/Users/vaibhav.sinha/Documents/Work/Learn/hudi/data/poc/users_orc2/last_login=2021-04-15");
    inputFormat.getSplits(conf, 2);
  }
}
