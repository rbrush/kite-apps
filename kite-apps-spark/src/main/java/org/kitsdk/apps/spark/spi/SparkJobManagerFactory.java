package org.kitsdk.apps.spark.spi;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.spi.jobs.JobManagerFactory;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitsdk.apps.spark.AbstractSchedulableSparkJob;

/**
 * Job manager for working with Spark jobs.
 */
public class SparkJobManagerFactory implements JobManagerFactory {

  @Override
  public boolean supports(Class jobClass) {

    return AbstractSchedulableSparkJob.class.isAssignableFrom(jobClass);
  }

  @Override
  public SchedulableJobManager createManager(Class jobClass, Configuration conf) {

    return SparkJobManager.create(jobClass, conf);
  }
}
