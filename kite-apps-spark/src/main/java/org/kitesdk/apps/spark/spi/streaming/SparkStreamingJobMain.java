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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.kitesdk.apps.spark.spi.DefaultKafkaContext;
import org.kitesdk.apps.spark.spi.DefaultSparkContext;
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

    String kafkaBrokers = args[0];
    String zkConnectionString = args[1];
    String jobDescription = args[2];

    // Parse the description passed to the launcher.
    StreamDescription descrip = StreamDescription.parseJson(jobDescription);

    System.out.println("RUNNING JOB:");
    System.out.println(jobDescription);

    SparkConf sparkConf = new SparkConf().setAppName(descrip.getJobClass().getName());

    // Create the spark context for the application.
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    // FIXME: what should we use for the duration here?
    JavaStreamingContext streamingContext = new JavaStreamingContext(ctx, new Duration(100000));

    // Set defaults for use in applications.
    DefaultSparkContext.setContext(ctx);
    DefaultSparkContext.setStreamingContext(streamingContext);
    DefaultKafkaContext.setKafkaBrokerList(kafkaBrokers);
    DefaultKafkaContext.setZookeeperConnectionString(zkConnectionString);

    // FIXME: set the Kafka broker list and zookeeper information.

    Configuration conf = ctx.hadoopConfiguration();

    // Use the loaded Hadoop configuration
    DefaultConfiguration.set(ctx.hadoopConfiguration());

    // Run the job.
    StreamingJobManager manager = JobManagers.createStreaming(descrip, ctx.hadoopConfiguration());
    manager.run();
  }
}
