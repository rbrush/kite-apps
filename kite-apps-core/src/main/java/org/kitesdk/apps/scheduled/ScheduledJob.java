package org.kitesdk.apps.scheduled;

import org.apache.hadoop.conf.Configurable;
import org.joda.time.Instant;

/**
 * Interface for scheduled Kite jobs.
 */
public interface ScheduledJob extends Configurable {

  /** Returns the nominal time at which the job is run. */
  Instant getNominalTime();

  /** Sets the nominal time at which the job is run. */
  void setNominalTime(Instant instant);
}
