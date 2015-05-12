package org.kitesdk.apps.scheduled;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;

/**
 * Abstract base class for scheduled jobs.
 */
public abstract class AbstractScheduledJob implements ScheduledJob {

  private Configuration configuration;

  private Instant nominalTime;

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public Instant getNominalTime() {
    return nominalTime;
  }

  @Override
  public void setNominalTime(Instant nominalTime) {
    this.nominalTime = nominalTime;
  }
}
