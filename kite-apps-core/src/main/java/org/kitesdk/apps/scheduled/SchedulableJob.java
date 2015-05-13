package org.kitesdk.apps.scheduled;

import org.apache.hadoop.conf.Configurable;
import org.joda.time.Instant;

/**
 * Interface for schedulable jobs. Implementations of this interface
 * must provide a nullary constructor.
 */
public interface SchedulableJob extends Configurable {

  /**
   * The name of the schedulable job, which may be the class name.
   * This name will be used in job-specific configuration files and
   * be visible in system management tooling.
   */
  String getName();

  /** Returns the nominal time at which the job is run. */
  Instant getNominalTime();

  /** Sets the nominal time at which the job is run. */
  void setNominalTime(Instant instant);
}
