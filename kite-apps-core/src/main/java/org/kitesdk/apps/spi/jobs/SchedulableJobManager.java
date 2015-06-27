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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.plexus.util.xml.XMLWriter;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.View;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Manager class for working with schedulable jobs.
 */
public abstract class SchedulableJobManager {

  protected final SchedulableJob job;

  protected final Configuration conf;

  protected final Method runMethod;

  protected SchedulableJobManager(SchedulableJob job,
                                  Method runMethod,
                                  Configuration conf) {
    this.job = job;
    this.runMethod = runMethod;
    this.conf = conf;
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

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();

    Map<String,DataIn> inputs = Maps.newHashMap();

    for (Annotation[] annotations: paramAnnotations) {
      for (Annotation annotation: annotations) {
        if (DataIn.class.equals(annotation.annotationType()))
          inputs.put(((DataIn) annotation).name(), (DataIn) annotation);
      }
    }

    return inputs;
  }

  /**
   * Gets the configuration used for the job.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Gets a map of job output names to the {@link DataOut} annotations
   * that declared them.
   */
  public Map<String,DataOut> getOutputs() {

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();

    Map<String,DataOut> outputs = Maps.newHashMap();

    for (Annotation[] annotations: paramAnnotations) {
      for (Annotation annotation: annotations) {
        if (DataOut.class.equals(annotation.annotationType()))
          outputs.put(((DataOut) annotation).name(), (DataOut) annotation);
      }
    }

    return outputs;
  }

  protected static Method resolveRunMethod(SchedulableJob job) {

    Method runMethod = null;

    for (Method method: job.getClass().getMethods()) {

      if ("run".equals(method.getName())) {

        if (runMethod != null)
          throw new AppException("Multiple run methods found on scheduled job "
              + job.getName());

        runMethod = method;
      }
    }

    if (runMethod == null)
      throw new AppException("Could not find run method on job "  +
          job.getName());

    return runMethod;
  }

  /**
   * Returns the arguments to be passed to a job's run method.
   */
  protected Object[] getArgs(Map<String,View> views) {

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();
    Object[] args = new Object[paramAnnotations.length];

    for (int i = 0; i < paramAnnotations.length; ++i) {

      Annotation[] annotations = paramAnnotations[i];

      for (Annotation annotation: annotations) {
        if (DataOut.class.equals(annotation.annotationType())) {

          View view = views.get(((DataOut) annotation).name());

          if (view == null) {
            throw new AppException("No view defined for job parameter: ");
          }

          args[i] = view;
        }

        if (DataIn.class.equals(annotation.annotationType())) {

          View view = views.get(((DataIn) annotation).name());

          if (view == null) {
            throw new AppException("No view defined for job parameter: ");
          }

          args[i] = view;
        }
      }
    }

    return args;
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
