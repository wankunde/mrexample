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

package com.wankun.mr.oldapi;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.*;

import static com.wankun.util.Utiltools.getStackTraceString;

/** Implements the identity function, mapping inputs directly to outputs.
 */
public class IdentityMapper<K, V>
        extends MapReduceBase implements Mapper<K, V, K, V> {

  private static final Log LOG = LogFactory.getLog(IdentityMapper.class);

  @Override
  public void configure(JobConf job) {
    LOG.info("mapper configure(). Stack : " + getStackTraceString(new Throwable()));
  }

  /** The identify function.  Input key/value pair is written directly to
   * output.*/
  public void map(K key, V val,
                  OutputCollector<K, V> output, Reporter reporter)
          throws IOException {
    output.collect(key, val);
  }

  @Override
  public void close() throws IOException {
    LOG.info("mapper close(). Stack : " + getStackTraceString(new Throwable()));
  }
}
