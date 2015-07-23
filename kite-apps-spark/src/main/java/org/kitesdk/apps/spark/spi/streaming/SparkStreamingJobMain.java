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
package org.kitesdk.apps.spark.spi.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.spi.PropertyFiles;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.StreamingJobManager;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.data.spi.DefaultConfiguration;

import java.util.Map;

/**
 * Main entrypoint for Spark Streaming jobs.
 */
public class SparkStreamingJobMain {

  public static void main(String[] args) throws Exception {

    String kiteAppRoot = args[0];

    String jobName = args[1];

    Configuration conf = getConf();

    // Use the loaded Hadoop configuration
    DefaultConfiguration.set(conf);

    FileSystem fs = FileSystem.get(conf);

    Path propertiesPath = new Path(kiteAppRoot, "conf/app.properties");
    Map<String,String> settings = PropertyFiles.loadIfExists(fs, propertiesPath);

    AppContext appContext = new AppContext(settings, conf);

    Path appPath = new Path(kiteAppRoot);

    StreamDescription descrip = SparkStreamingJobManager.loadDescription(fs, appPath, jobName);

    // Run the job.
    StreamingJobManager manager = JobManagers.createStreaming(descrip, appContext);
    manager.run();
  }

  private static Configuration getConf() {

    // TODO: create spark configuration to load Hadoop configuration resources.
    // Is there a cleaner way to do this?
    SparkConf sparkConf = new SparkConf().setAppName("PLACEHOLDER");

    // Create the spark context for the application.
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    Configuration conf = ctx.hadoopConfiguration();

    ctx.close();

    return conf;
  }
}
