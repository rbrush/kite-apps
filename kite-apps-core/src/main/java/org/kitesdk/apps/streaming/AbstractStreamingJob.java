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
package org.kitesdk.apps.streaming;


import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.JobParameters;
import org.kitesdk.apps.spi.jobs.JobReflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Abstract class for streaming jobs.
 */
public abstract class AbstractStreamingJob implements StreamingJob<JobContext> {


  private JobContext context;

  /**
   * Gets the context for the job.
   */
  public JobContext getJobContext() {
    return context;
  }

  @Override
  public final JobParameters getParameters() {
    return JobReflection.getParameters(getClass());
  }

  @Override
  public final void runJob(Map params, JobContext context) {

    this.context = context;

    Method runMethod = JobReflection.resolveRunMethod(getClass());

    Object[] args = JobReflection.getArgs(runMethod, params);

    try {

      runMethod.invoke(this, args);

    } catch (IllegalAccessException e) {
      throw new AppException(e);
    } catch (InvocationTargetException e) {
      throw new AppException(e);
    }
  }
}
