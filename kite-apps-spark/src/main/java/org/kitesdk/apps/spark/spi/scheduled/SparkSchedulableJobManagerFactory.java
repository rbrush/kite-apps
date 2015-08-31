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
package org.kitesdk.apps.spark.spi.scheduled;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spark.AbstractSchedulableSparkJob;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.spi.jobs.SchedulableJobManagerFactory;

/**
 * Job manager for working with Spark jobs.
 */
public class SparkSchedulableJobManagerFactory implements SchedulableJobManagerFactory {

  @Override
  public boolean supports(Class jobClass) {

    return AbstractSchedulableSparkJob.class.isAssignableFrom(jobClass);
  }

  @Override
  public SchedulableJobManager createManager(Schedule schedule, AppContext context) {

    return SparkJobManager.create(schedule, context);
  }
}
