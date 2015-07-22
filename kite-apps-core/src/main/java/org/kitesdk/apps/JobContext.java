/**
 * Copyright 2015 Cerner Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.apps;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.Map;

/**
 * Context in which a Kite job is running.
 */
public class JobContext {

  private final Configuration conf;

  private final Map<String,String> settings;

  /**
   * Gets the configuration settings for the job.
   */
  public Map<String,String> getSettings() {
    return settings;
  }

  /**
   * Gets the Hadoop configuration for the job.
   */
  public Configuration getHadoopConf() {
    return new Configuration(conf);
  }

  /**
   * Creates a context with the given settings and Hadoop configuration.
   */
  public JobContext(Map<String,String> settings, Configuration conf) {
    this.settings = settings;
    this.conf = conf;
  }

}
