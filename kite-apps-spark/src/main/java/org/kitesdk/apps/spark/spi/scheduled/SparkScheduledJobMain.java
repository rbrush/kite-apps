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
import org.joda.time.Instant;
import org.kitesdk.apps.AppContext;
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

    String jobClassName = args[0];
    String jobName = args[1];

    SparkConf sparkConf = new SparkConf().setAppName(jobClassName);

    // Create the spark context for the application.
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    Configuration conf = ctx.hadoopConfiguration();

    // Use the loaded Hadoop configuration
    DefaultConfiguration.set(ctx.hadoopConfiguration());

    String kiteAppRoot = conf.get("kiteAppRoot");

    Path propertiesPath = new Path(kiteAppRoot, "conf/app.properties");

    Map<String,String> settings = PropertyFiles.loadIfExists(FileSystem.get(conf), propertiesPath);

    AppContext appContext = new AppContext(settings, conf);

    Instant nominalTime = OozieScheduling.getNominalTime(conf);

    ClassLoader loader = SparkScheduledJobMain.class.getClassLoader();

    Class jobClass = loader.loadClass(jobClassName);

    SchedulableJobManager manager = JobManagers.createSchedulable(jobClass, jobName, appContext);

    // Get the views to be used from Oozie configuration.
    Map<String, View> views = OozieScheduling.loadViews(manager, conf);

    manager.run(nominalTime, views);
  }
}
