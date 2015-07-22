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
package org.kitesdk.apps.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spi.jobs.JobUtil;

/**
 * Context for spark-based jobs.
 */
public class SparkJobContext extends JobContext {


  AppContext appContext;

  public JavaSparkContext getSparkContext() {

    return SparkContextFactory.getSparkContext(appContext);
  }

  public JavaStreamingContext getSparkStreamingContext() {

    return SparkContextFactory.getStreamingContext(appContext);
  }

  /**
   * Creates a context with the given settings and Hadoop configuration.
   */
  public SparkJobContext(String jobName, AppContext context) {
    super(JobUtil.toJobSettings(jobName, context),
        JobUtil.toJobHadoopConf(jobName, context));

    this.appContext = context;
  }
}
