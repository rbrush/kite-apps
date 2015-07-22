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
import org.joda.time.Instant;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.scheduled.SchedulableJob;

/**
 * Abstract base class for a schedulable Spark job.
 */
public abstract class AbstractSchedulableSparkJob  implements SchedulableJob<SparkJobContext> {

  private SparkJobContext context;

  private Instant nominalTime;

  @Override
  public Instant getNominalTime() {
    return nominalTime;
  }

  @Override
  public void setNominalTime(Instant nominalTime) {

    this.nominalTime = nominalTime;
  }

  @Override
  public void setJobContext(SparkJobContext context) {
    this.context = context;
  }

  @Override
  public SparkJobContext getJobContext() {
    return context;
  }
}
