package org.kitsdk.apps.spark.spi;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.Instant;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.spi.oozie.OozieScheduling;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DefaultConfiguration;

import java.util.Map;

/**
 * Main entrypoint for Spark jobs.
 */
public class SparkScheduledJobMain {

  public static void main(String[] args) throws Exception {

    String jobClassName = args[0];

    SparkConf sparkConf = new SparkConf().setAppName(jobClassName);

    // Create the spark context for the application.
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    DefaultSparkContext.setContext(ctx);

    Configuration conf = ctx.hadoopConfiguration();

    // Use the loaded Hadoop configuration
    DefaultConfiguration.set(ctx.hadoopConfiguration());

    Instant nominalTime = OozieScheduling.getNominalTime(conf);

    ClassLoader loader = SparkScheduledJobMain.class.getClassLoader();

    Class jobClass = loader.loadClass(jobClassName);

    SchedulableJobManager manager = JobManagers.create(jobClass, conf);

    // Get the views to be used from Oozie configuration.
    Map<String, View> views = OozieScheduling.loadViews(manager, conf);

    manager.run(nominalTime, views);
  }
}
