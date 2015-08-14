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
package org.kitesdk.apps.scheduled;

import org.joda.time.Instant;
import org.kitesdk.apps.JobContext;

/**
 * Abstract base class for schedulable jobs.
 */
public abstract class AbstractSchedulableJob implements SchedulableJob<JobContext> {

  private JobContext context;

  private Instant nominalTime;

  @Override
  public Instant getNominalTime() {
    return nominalTime;
  }

  @Override
  public void setNominalTime(Instant nominalTime) {
    this.nominalTime = nominalTime;
  }

  /**
   * Sets the context for the job. This is called before the job is run.
   */
  public void setJobContext(JobContext jobContext) {
    this.context = jobContext;
  }

  /**
   * Gets the context for the job.
   */
  public JobContext getJobContext() {
    return context;
  }
}
