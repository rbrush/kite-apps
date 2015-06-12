package org.kitesdk.apps.spi.jobs;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory for creating job managers that support the given implementation class.
 */
public interface JobManagerFactory {

  public abstract boolean supports(Class jobClass);

  public abstract SchedulableJobManager createManager(Class jobClass, Configuration conf);
}
