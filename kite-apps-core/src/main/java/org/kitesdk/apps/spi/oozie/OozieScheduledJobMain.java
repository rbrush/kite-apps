/**
 * Copyright 2015 Cerner Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.apps.spi.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.Instant;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.spi.PropertyFiles;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The main entry point invoked by Oozie as a Java action to launch jobs.
 */
public class OozieScheduledJobMain extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(OozieScheduledJobMain.class);

  public static void main(String [] args) throws Exception {

    try {
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

      ToolRunner.run(conf, new OozieScheduledJobMain(), args);

    } catch (Exception e) {

      // Spark actions do a poor job of reporting errors, so
      // we have this here to at least have a log of the root cause.
      LOG.error("Unhandled exception in application.", e);

      throw e;
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    String kiteAppRoot = getConf().get("kiteAppRoot");

    Path propertiesPath = new Path(kiteAppRoot, "conf/app.properties");

    Map<String,String> settings = PropertyFiles.loadIfExists(FileSystem.get(getConf()), propertiesPath);

    AppContext appContext = new AppContext(settings, getConf());

    Instant nominalTime = OozieScheduling.getNominalTime(getConf());

    String jobClassName = args[0];

    ClassLoader loader = OozieScheduledJobMain.class.getClassLoader();

    Class jobClass = loader.loadClass(jobClassName);

    SchedulableJobManager manager = JobManagers.createSchedulable(jobClass, appContext);

    // Use the configuration customized for the job.
    Configuration conf = manager.getJobContext().getHadoopConf();

    DefaultConfiguration.set(conf);

    // Get the views to be used from Oozie configuration.
    Map<String, View> views = OozieScheduling.loadViews(manager, conf);

    manager.run(nominalTime, views);

    return 0;
  }
}
