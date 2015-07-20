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
package org.kitesdk.apps.spark.spi.streaming;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.spark.AbstractStreamingSparkJob;
import org.kitesdk.apps.spi.jobs.StreamingJobManager;
import org.kitesdk.apps.spi.jobs.StreamingJobManagerFactory;
import org.kitesdk.apps.streaming.StreamDescription;

/**
 * Manager factory for spark streaming job.
 */
public class SparkStreamingJobManagerFactory implements StreamingJobManagerFactory {
  @Override
  public boolean supports(Class jobClass) {
    return AbstractStreamingSparkJob.class.isAssignableFrom(jobClass);
  }

  @Override
  public StreamingJobManager createManager(StreamDescription description, Configuration conf) {

    return SparkStreamingJobManager.create(description, conf);
  }
}
