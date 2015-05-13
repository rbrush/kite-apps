package org.kitesdk.apps.scheduled;

import com.google.common.collect.Maps;
import org.joda.time.Instant;
import org.kitesdk.apps.spi.ScheduledJobUtil;

import java.util.Collections;
import java.util.Map;

/**
 * A schedule for a job.
 */
public class Schedule {

  private final Class<? extends ScheduledJob> jobClass;

  private final String frequency;

  private final String name;

  private final Instant startTime;

  private final Map<String,ViewTemplate> views;

  Schedule(Class<? extends ScheduledJob> jobClass, String name, String frequency,
           Instant startTime, Map<String,ViewTemplate> views) {
    this.jobClass = jobClass;
    this.name = name;
    this.frequency = frequency;
    this.startTime = startTime;
    this.views = Collections.unmodifiableMap(views);
  }

  public Class<? extends ScheduledJob> getJobClass() {
    return jobClass;
  }

  public String getFrequency() {
    return frequency;
  }

  public String getName() {return name; }

  public Instant getStartTime() { return startTime; }

  public Map<String,ViewTemplate> getViewTemplates() { return views; }

  /**
   * A template for views used by the scheduled job.
   */
  public static class ViewTemplate {

    private final String name;
    private final String uriTemplate;
    private final int frequency;
    private final Class inputType;

    ViewTemplate(String name, String uriTemplate, Class inputType, int frequency) {
      this.name = name;
      this.uriTemplate = uriTemplate;
      this.inputType = inputType;
      this.frequency = frequency;
    }

    public String getName() {
      return name;
    }

    public String getUriTemplate() {
      return uriTemplate;
    }

    public int getFrequency() {
      return frequency;
    }

    public Class getInputType() {
      return inputType;
    }
  }

  public static class Builder {

    private Class jobClass = null;

    private String frequency = null;

    private String name;

    private Instant startTime = new Instant();

    private Map<String,ViewTemplate> views = Maps.newHashMap();

    public Builder jobClass(Class jobClass) {

      this.jobClass = jobClass;

      // Use the job class for the schedule name unless otherwise
      // specified.
      if (name == null) {
        name = jobClass.getName();
      }

      return this;
    }

    public Builder name(String name) {

      this.name = name;
      return this;
    }

    public Builder hourly() {

      return frequency("0 * * * *");
    }

    public Builder frequency(String cronFrequency) {

      frequency = cronFrequency;

      return this;
    }

    public Builder withView(String name, String uriTemplate, int frequencyMinutes) {

      Map<String,DataIn> inputs = ScheduledJobUtil.getInputs(jobClass);

      Map<String,DataOut> outputs = ScheduledJobUtil.getOutputs(jobClass);

      Class type = inputs.containsKey(name) ? inputs.get(name).type() :
          outputs.containsKey(name) ? outputs.get(name).type() : null;

      if (type == null)
        throw new IllegalArgumentException("Named parameters " + name +
            " not used in job " + jobClass.getName());

      views.put(name, new ViewTemplate(name, uriTemplate, type, frequencyMinutes));

      return this;
    }

    public Schedule build() {

      return new Schedule(jobClass, name, frequency, startTime, views);
    }
  }
}
