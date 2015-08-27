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

import org.apache.avro.Schema;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.JobParameters;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.spi.jobs.JobReflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for a schedulable Spark job.
 */
public abstract class AbstractSchedulableSparkJob implements SchedulableJob<SparkJobContext> {

  private SparkJobContext context;

  private Instant nominalTime;

  /**
   * Returns a list of Avro schemas used by the job so they can be registered
   * with the underlying serialization framework. By default this method simply
   * inspects the {@link org.kitesdk.apps.DataIn} and
   * {@link org.kitesdk.apps.DataOut} annotations for Avro types,
   * but users may override this if further sharing is needed.
   */
  public List<Schema> getUsedSchemas() {

    return JobReflection.getSchemas(this);
  }

  protected final Instant getNominalTime() {
    return nominalTime;
  }

  protected final SparkJobContext getJobContext() {
    return context;
  }

  @Override
  public JobParameters getParameters() {
    return JobReflection.getParameters(getClass());
  }

  public final void runJob(Map<String,?> params, SparkJobContext jobContext, Instant nominalTime) {

    this.context = jobContext;
    this.nominalTime = nominalTime;

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
