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

import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobParameters;
import org.kitesdk.apps.spi.jobs.JobReflection;
import org.kitesdk.apps.spi.oozie.CronConverter;
import parquet.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A schedule for a job. See the {@link Schedule.Builder}
 * for details on building the schedules.
 */
public class Schedule {

  private final Class<? extends SchedulableJob> jobClass;

  private final String frequency;

  private final String name;

  private final Instant startTime;

  private final Map<String,ViewTemplate> views;

  /**
   * Job names should be within Oozie constraints.
   */
  private static final Pattern NAME_PATTERN = Pattern.compile("([a-zA-Z]([\\-_a-zA-Z0-9])*){1,39}");

  Schedule(Class<? extends SchedulableJob> jobClass, String name, String frequency,
           Instant startTime, Map<String,ViewTemplate> views) {
    this.jobClass = jobClass;
    this.name = name;
    this.frequency = frequency;
    this.startTime = startTime;
    this.views = Collections.unmodifiableMap(views);
  }

  /**
   * Gets the class of the {@link SchedulableJob}
   * being scheduled.
   *
   * @return the job class
   */
  public Class<? extends SchedulableJob> getJobClass() {
    return jobClass;
  }

  /**
   * Gets the frequency of the job in CRON form.
   *
   * @return the job frequency
   */
  public String getFrequency() {
    return frequency;
  }

  /**
   * Gets the name of the schedule, which may be visible in Oozie
   * or other tooling on the running system.
   *
   * @return the schedule name
   */
  public String getName() { return name; }

  /**
   * Gets the time at which first instance of a scheduled job
   * can start running.
   *
   * @return the job start time
   */
  public Instant getStartTime() { return startTime; }

  /**
   * Returns a map where the keys are the names of {@link org.kitesdk.apps.DataIn} or
   * {@link org.kitesdk.apps.DataOut} parameters in a job. The map
   * values are {@link org.kitesdk.apps.scheduled.Schedule.ViewTemplate}
   * objects that define the input pattern and type.
   *
   * @return a map of input and output names to templates.
   */
  public Map<String,ViewTemplate> getViewTemplates() { return views; }

  /**
   * A template for views used by the scheduled job.
   */
  public static class ViewTemplate {

    private final String name;
    private final String uriTemplate;
    private final String frequency;
    private final Class inputType;

    ViewTemplate(String name, String uriTemplate, Class inputType, String frequency) {
      this.name = name;
      this.uriTemplate = uriTemplate;
      this.inputType = inputType;
      this.frequency = frequency;
    }

    /**
     * Gets the name, which matches the associated {@link org.kitesdk.apps.DataIn}
     * or {@link org.kitesdk.apps.DataOut} annotation.
     *
     * @return the name of the template
     */
    public String getName() {
      return name;
    }

    /**
     * Gets the Oozie-compatible URI template used to define the schedule.
     *
     * @return the URI template
     */
    public String getUriTemplate() {
      return uriTemplate;
    }

    /**
     * Gets the cron-style frequency for which a view is expected to
     * arrive.
     *
     * @return the data arrival frequency for the view.
     */
    public String getFrequency() {
      return frequency;
    }

    /**
     * Gets the type of the object used by the view, typically
     * an Avro generic record or specific record implementation.
     *
     * @return the type
     */
    public Class getInputType() {
      return inputType;
    }
  }

  /**
   * A fluent builder to createSchedulable {@link Schedule} instances.
   */
  public static class Builder {

    private Class jobClass = null;

    private JobParameters params = null;

    private String frequency = null;

    private String name;

    private Instant startTime = new Instant();

    private Map<String,ViewTemplate> views = Maps.newHashMap();

    /**
     * Sets the name of the schedulable job. This name will be used in job-specific
     * configuration files and visible in system management tooling.

     */
    public Builder jobName(String jobName) {

      if (!NAME_PATTERN.matcher(jobName).matches())
        throw new AppException("Job name " + jobName + " must match pattern " + NAME_PATTERN + ".");

      this.name = jobName;
      return this;
    }

    /**
     * Sets the class of the {@link org.kitesdk.apps.scheduled.SchedulableJob}
     * being scheduled.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder jobClass(Class jobClass) {

      this.jobClass = jobClass;

      this.params = JobReflection.createSchedulableJob(jobClass).getParameters();

      return this;
    }

    /**
     * Instructs the job to start after the given start time. The actual start
     * time of the job will be the earliest instant at or after the given
     * start time that aligns with the job's schedule. For instance,
     * of the job schedule is "0 * * * *", indicating running at the
     * start of each hour, the effective time will be at the start
     * of the first our after the given time.
     *
     * @param startTime the time at which the job may start
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder startAt(Instant startTime) {

      this.startTime = startTime;

      return this;
    }

    /**
     * Sets an hourly frequency on the schedule, equivalent to calling
     * <code>builder.frequency("0 * * * *");(</code>
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder hourly() {

      return frequency("0 * * * *");
    }

    /**
     * Sets a cron-style frequency on the schedule. This frequency uses the
     * structure "minute hour day-of-month month day-of-week", which may be
     * values or wild cards. For instance, "0 * * * *" runs at minute zero
     * of every hour.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder frequency(String cronFrequency) {

      frequency = cronFrequency;

      return this;
    }

    /**
     * Deprecated. Configures a view defined by a name and URI template to be used
     * by the scheduled job.
     *
     * @param name the name of the data parameter for the job.
     * @param uriTemplate the Oozie-style URI template identifying the input
     * @param frequencyMinutes the frequency in minutes with which instances of
     *                         the view are expected to be created.
     *
     * @return An instance of the builder for method chaining.
     */
    @Deprecated
    public Builder withView(String name, String uriTemplate, int frequencyMinutes) {

      Class type = params.getRecordType(name);

      if (type == null)
        throw new IllegalArgumentException("Named parameters " + name +
            " not used in job " + jobClass.getName());

      String cronFrequency = Integer.toString(frequencyMinutes) + " * * * *";

      views.put(name, new ViewTemplate(name, uriTemplate, type, cronFrequency));

      return this;
    }

    private void checkParamType(String name, Class recordType) {

      if (!params.getRecordType(name).equals(recordType))
        throw new IllegalArgumentException("Job expected parameter " + name +
            " to be of type " + params.getRecordType(name).getName() +
            " but was given " + recordType.getName());
    }

    /**
     * If the job specifies input parameters, check that the given name
     * is included.
     */
    private void checkInputParam(String name, Class recordType) {

      if (params != null) {

        if (!params.getInputNames().contains(name))
          throw new IllegalArgumentException("Named input parameter " + name +
              " not used in job " + jobClass.getName());

        checkParamType(name, recordType);
      }
    }

    /**
     * If the job specifies input parameters, check that the given name
     * is included.
     */
    private void checkOutputParam(String name, Class recordType) {

      if (params != null) {

        if (!params.getOutputNames().contains(name))
          throw new IllegalArgumentException("Named output parameter " + name +
              " not used in job " + jobClass.getName());

        checkParamType(name, recordType);
      }
    }

    private Class getRecordType(String name) {

      return params != null ? params.getRecordType(name) : GenericRecord.class;
    }

    /**
     * Configures a view defined by a name and URI template to be used
     * as an input for the scheduled job. The caller must also specify
     * a cron-style frequency for when data for that input arrives.
     *
     * @param name the name of the data parameter for the job.
     * @param uriTemplate the Oozie-style URI template identifying the input
     * @param cronFrequency the frequency in minutes with which instances of
     *                         the view are expected to be created.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder withInput(String name, String uriTemplate, String cronFrequency) {

      return withInput(name, uriTemplate, cronFrequency,  getRecordType(name));
    }

    /**
     * Configures a view defined by a name and URI template to be used
     * as an input for the scheduled job. The caller must also specify
     * a cron-style frequency for when data for that input arrives.
     *
     * @param name the name of the data parameter for the job.
     * @param uriTemplate the Oozie-style URI template identifying the input
     * @param cronFrequency the frequency in minutes with which instances of
     *                         the view are expected to be created.
     * @param recordType The class of the record used by the view
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder withInput(String name, String uriTemplate, String cronFrequency, Class recordType) {

      checkInputParam(name, recordType);

      views.put(name, new ViewTemplate(name, uriTemplate, recordType, cronFrequency));

      return this;
    }

    /**
     * Configures a view defined by a name and URI template to be used as
     * an output the scheduled job. The frequency for the output is the
     * same for the job itself.
     *
     * @param name the name of the data parameter for the job.
     * @param uriTemplate the Oozie-style URI template identifying the input
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder withOutput(String name, String uriTemplate) {

      return withOutput(name, uriTemplate, getRecordType(name));
    }

    /**
     * Configures a view defined by a name and URI template to be used as
     * an output the scheduled job. The frequency for the output is the
     * same for the job itself.
     *
     * @param name the name of the data parameter for the job.
     * @param uriTemplate the Oozie-style URI template identifying the input
     * @param recordType The class of the record used by the view
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder withOutput(String name, String uriTemplate, Class recordType) {

      checkOutputParam(name, recordType);

      views.put(name, new ViewTemplate(name, uriTemplate, recordType, frequency));

      return this;
    }

    /**
     * Builds the schedule, returning an immutable {@link Schedule} instance
     * that can be used to schedule jobs.
     *
     * @return a Schedule.
     */
    public Schedule build() {

      // If the job specified its parameters, make sure everything is in place.
      if (params != null) {

        for (String viewName: params.getInputNames()) {
          if (!views.containsKey(viewName))
            throw new IllegalArgumentException("Named input " + viewName + " not provided in schedule");
        }

        for (String viewName: params.getOutputNames()) {
          if (!views.containsKey(viewName))
            throw new IllegalArgumentException("Named output " + viewName + " not provided in schedule");
        }
      }


      Preconditions.checkNotNull(jobClass, "Job class must be provided.");
      Preconditions.checkNotNull(name, "Job name must be provided.");

      // Use the next start time that aligns with the cron schedule.
      Instant effectiveStartTime = CronConverter.nextInstant(frequency, startTime);

      return new Schedule(jobClass, name, frequency, effectiveStartTime, views);
    }
  }
}
