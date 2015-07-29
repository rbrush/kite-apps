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
package org.kitesdk.apps.spi.jobs;

import org.codehaus.plexus.util.xml.XMLWriter;
import org.joda.time.Instant;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.View;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * Manager class for working with schedulable jobs.
 */
public abstract class SchedulableJobManager {

  protected final SchedulableJob job;

  protected final AppContext context;

  protected final Method runMethod;

  protected SchedulableJobManager(SchedulableJob job,
                                  Method runMethod,
                                  AppContext context) {
    this.job = job;
    this.runMethod = runMethod;
    this.context = context;
  }

  /**
   * Gets the name of the scheduled job.
   *
   * @return the name of the scheduled job
   */
  public String getName() {
    return job.getName();
  }

  /**
   * Gets a map of job input names to the {@link DataIn} annotations
   * that declared them.
   */
  public Map<String, DataIn> getInputs() {

    return JobReflection.getInputs(runMethod);
  }

  /**
   * Gets the application context used with this job.
   */
  public AppContext getAppContext() {
    return context;
  }

  /**
   * Returns the context used by the job.
   */
  public abstract JobContext getJobContext();

  /**
   * Gets a map of job output names to the {@link DataOut} annotations
   * that declared them.
   */
  public Map<String,DataOut> getOutputs() {

    return JobReflection.getOutputs(runMethod);
  }

  /**
   * Signal the produced views as ready for downstream processing.
   */
  protected void signalOutputViews(Map<String,View> views) {

    Set<String> outputNames = getOutputs().keySet();

    for (String outputName: outputNames) {

      View view = views.get(outputName);

      if (view instanceof Signalable) {

        ((Signalable) view).signalReady();
      }
    }
  }

  /**
   * Runs the job at the given nominal time with the given
   * input and output views.
   *
   * @param nominalTime the nominal time provided to the job
   * @param views a map of view parameter names to loaded instances for the job.
   */
  public abstract void run(Instant nominalTime, Map<String,View> views);

  /**
   * Writes the Oozie action block to launch the job. This allows for
   * different types of jobs to use specialized action blocks. For instance,
   * Crunch jobs may use a Java action and Spark jobs could use a Spark action.
   */
  public abstract void writeOozieActionBlock(XMLWriter writer, Schedule schedule);
}
