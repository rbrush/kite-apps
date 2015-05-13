package org.kitesdk.apps.spi.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.spi.SchedulableJobManager;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DefaultConfiguration;

import java.util.Map;

/**
 * Invoked by Oozie to run scheduled jobs.
 */
public class OozieScheduledJobMain extends Configured implements Tool {

  public static void main(String [] args) throws Exception {

    GenericOptionsParser options = new GenericOptionsParser(args);

    // Include Oozie-provided configuration if it is specified.
    String configurationLocation = System.getProperty("oozie.action.conf.xml");

    if (configurationLocation == null) {
      throw new AppException("No oozie.action.conf.xml set; cannot resolve configuration.");
    }

    Configuration conf = options.getConfiguration();

    // The configuration location appears to be a location on the filesystem,
    // hence the necessary prefix.
    conf.addResource(new Path("file://" + configurationLocation));

    DefaultConfiguration.set(conf);

    ToolRunner.run(conf, new OozieScheduledJobMain(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    Instant nominalTime = OozieScheduling.getNominalTime(getConf());

    String jobClassName = args[0];

    ClassLoader loader = OozieScheduledJobMain.class.getClassLoader();

    Class jobClass = loader.loadClass(jobClassName);

    SchedulableJobManager manager = SchedulableJobManager.create(jobClass, getConf());

    // Get the views to be used from Oozie configuration.
    Map<String, View> views = OozieScheduling.getViews(manager, getConf());

    manager.run(nominalTime, views);

    return 0;
  }
}
