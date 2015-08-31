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

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.plexus.util.xml.XMLWriter;
import org.joda.time.Instant;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.apps.JobParameters;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.View;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * Manager class for working with schedulable jobs.
 */
public abstract class SchedulableJobManager {

  protected final SchedulableJob job;

  protected final AppContext context;

  protected final Schedule schedule;

  protected SchedulableJobManager(SchedulableJob job,
                                  Schedule schedule,
                                  AppContext context) {
    this.job = job;
    this.schedule = schedule;
    this.context = context;
  }

  /**
   * Gets the name of the scheduled job.
   *
   * @return the name of the scheduled job
   */
  public String getName() {
    return schedule.getName();
  }


  /**
   * Gets the schedule for the job.
   */
  public Schedule getSchedule() {
    return schedule;
  }

  /**
   * Returns the input and output parameters used to run the job.
   */
  public JobParameters getJobParameters() {

    return job.getParameters();
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
   * Signal the produced views as ready for downstream processing.
   */
  protected void signalOutputViews(Map<String,View> views) {

    // If the job specified output parameters,
    // signal them when we complete.
    JobParameters params = getJobParameters();

    if (params != null) {

      Set<String> outputNames = getJobParameters().getOutputNames();

      for (String outputName: outputNames) {

        View view = views.get(outputName);

        if (view instanceof Signalable) {

          ((Signalable) view).signalReady();
        }
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

  /**
   * Returns the path of the scheduled job file.
   */
  public static Path scheduleFile(Path appRoot, String jobName) {
    return new Path (appRoot, "schedules/" + jobName + ".json");
  }

  /**
   * Loads the schedule for the given application root.
   */
  public static Schedule loadSchedule(FileSystem fs, Path appRoot, String jobName)  {

    Path streamingJobPath = scheduleFile(appRoot, jobName);

    StringBuilder builder = new StringBuilder();
    try {
      InputStream input = fs.open(streamingJobPath);

      InputStreamReader streamReader = new InputStreamReader(input, Charsets.UTF_8);

      BufferedReader reader = new BufferedReader(streamReader);

      try {

        String line;

        while ((line = reader.readLine()) != null) {

          builder.append(line);
        }
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new AppException(e);
    }

    return Schedule.parseJson(builder.toString());
  }
}
