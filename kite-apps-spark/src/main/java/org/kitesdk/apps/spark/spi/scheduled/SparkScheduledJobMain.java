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
package org.kitesdk.apps.spark.spi.scheduled;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.joda.time.Instant;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spi.PropertyFiles;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.spi.oozie.OozieScheduling;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DefaultConfiguration;

import java.util.Collections;
import java.util.Map;

/**
 * Main entrypoint for Spark jobs.
 */
public class SparkScheduledJobMain {

  public static void main(String[] args) throws Exception {

    String jobName = args[0];

    SparkConf sparkConf = new SparkConf().setAppName(jobName);

    // Get the Hadoop configuration so we can load our settings application
    // settings. We use the SparkConf to grab the provided configuration.
    Configuration conf = SparkHadoopUtil.get().newConfiguration(sparkConf);

    // Use the loaded Hadoop configuration
    DefaultConfiguration.set(conf);

    String kiteAppRoot = conf.get("kiteAppRoot");

    Path propertiesPath = new Path(kiteAppRoot, "conf/app.properties");

    FileSystem fs = FileSystem.get(conf);

    Map<String,String> settings = PropertyFiles.loadIfExists(fs, propertiesPath);

    AppContext appContext = new AppContext(settings, conf);

    Instant nominalTime = OozieScheduling.getNominalTime(conf);

    Schedule schedule = SchedulableJobManager.loadSchedule(fs, new Path(kiteAppRoot), jobName);

    SchedulableJobManager manager = JobManagers.createSchedulable(schedule, appContext);

    // Get the views to be used from Oozie configuration.
    Map<String, View> views = OozieScheduling.loadViews(manager, conf);

    manager.run(nominalTime, views);
  }
}
