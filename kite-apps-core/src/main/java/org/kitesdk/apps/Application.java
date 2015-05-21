package org.kitesdk.apps;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.scheduled.Schedule;

import java.util.List;

/**
 * Kite applications are defined by implementations of this interface.
 *
 * <p>
 * Applications must implement a {@link #setup} method, in which they
 * can perform arbitrary installation steps for the application. This can
 * include creating Kite datasets, importing existing data, or performing
 * any initialization needed for the application.
 * </p>
 * <p>
 * Applications typically schedule one or more jobs to do work based
 * on either a time-based schedule or the arrival of data. See the
 * {@link org.kitesdk.apps.scheduled.Schedule} and
 * {@link org.kitesdk.apps.scheduled.SchedulableJob} interfaces
 * for details.
 * </p>
 */
public interface Application {

  /**
   * Sets up the application.  This can perform arbitrary
   * operations such as installing datasets, importing data,
   * or configuring scheduled jobs.
   *
   * @param conf Hadoop configuration for the environment
   */
  public void setup(Configuration conf);

  /**
   * Gets the list of schedules for jobs that are launched
   * by this application.
   *
   * @return a list of schedules
   */
  public List<Schedule> getSchedules();
}
