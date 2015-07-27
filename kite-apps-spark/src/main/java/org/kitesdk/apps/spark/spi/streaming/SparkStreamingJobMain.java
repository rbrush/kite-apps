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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.spark.AbstractStreamingSparkJob;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spark.spi.kryo.KryoAvroRegistrator;
import org.kitesdk.apps.spi.PropertyFiles;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.StreamingJobManager;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Main entrypoint for Spark Streaming jobs.
 */
public class SparkStreamingJobMain {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SparkStreamingJobMain.class);


  public static void main(String[] args) throws Exception {

    String kiteAppRoot = args[0];

    String jobName = args[1];

    FileSystem fs = FileSystem.get(new Configuration());

    Path appPath = new Path(kiteAppRoot);

    StreamDescription descrip = SparkStreamingJobManager.loadDescription(fs, appPath, jobName);

    Path propertiesPath = new Path(kiteAppRoot, "conf/app.properties");
    Map<String,String> settings = PropertyFiles.loadIfExists(fs, propertiesPath);

    // Create the spark context for the application.
    JavaSparkContext context = SparkContextFactory.getSparkContext(settings);

    Configuration conf = context.hadoopConfiguration();

    // Use the loaded Hadoop configuration
    DefaultConfiguration.set(conf);

    AppContext appContext = new AppContext(settings, conf);


    // Run the job.
    StreamingJobManager manager = JobManagers.createStreaming(descrip, appContext);
    manager.run();

    JavaStreamingContext streamingContext = SparkContextFactory.getStreamingContext(settings);

    streamingContext.start();
    streamingContext.awaitTermination();
  }
}
