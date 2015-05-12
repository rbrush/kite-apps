package org.kitesdk.apps.scheduled;

import com.google.common.collect.Maps;
import org.joda.time.Instant;

import java.util.Collections;
import java.util.Map;

/**
 * A schedule for a KiteJob.
 */
public class Schedule {

  private final Class<? extends ScheduledJob> jobClass;

  private final String frequency;

  private final String name;

  private final Instant startTime;

  private final Map<String,Input> views;

  Schedule(Class<? extends ScheduledJob> jobClass, String name, String frequency,
           Instant startTime, Map<String,Input> views) {
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

  public Map<String,Input> getInputs() { return views; }

  public static class Input {

    private final String name;
    private final String uriTemplate;
    private final int frequency;

    Input (String name, String uriTemplate, int frequency) {
      this.name = name;
      this.uriTemplate = uriTemplate;
      this.frequency = frequency;
    }

    public String getName() {
      return name;
    }

    public String getUriTemplate() {
      return uriTemplate;
    }

    // TODO: Frequence for Oozie datasets is a time interval, not a cron schedule.
    // This inconsistency is confusing.
    public int getFrequency() {
      return frequency;
    }
  }

  public static class Builder {

    private Class jobClass = null;

    private String frequency = null;

    private String name;

    private Instant startTime = new Instant();

    private Map<String,Input> views = Maps.newHashMap();

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

    // TODO: remove this? don't think we need it in addition to withView.
    public Builder onReady(String name, String uriPattern, int frequencyMinutes) {

      views.put(name, new Input(name, uriPattern, frequencyMinutes));

      return this;
    }

    public Builder withView(String name, String uriPattern, int frequencyMinutes) {

      views.put(name, new Input(name, uriPattern, frequencyMinutes));

      return this;
    }

    public Schedule build() {

      return new Schedule(jobClass, name, frequency, startTime, views);
    }
  }
}
