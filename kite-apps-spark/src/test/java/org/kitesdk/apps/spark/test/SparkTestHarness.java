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
package org.kitesdk.apps.spark.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.ManualClock;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.Application;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spi.jobs.JobUtil;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.apps.streaming.StreamingJob;
import org.kitesdk.data.View;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Helper class to test stream-based applications
 */
public class SparkTestHarness {

  private final Application app;

  private final Configuration conf;

  private final AppContext context;

  private final JavaStreamingContext streamingContext;

  private final ManualClock clock;

  SparkTestHarness(Application app, Configuration conf) {
    this.app = app;
    this.conf = conf;

    Map<String,String> settings = ImmutableMap.<String,String>builder()
        .put("spark.app.name", "spark-test")
        .put("spark.master", "local[3]")
        .put("spark.streaming.clock", "org.apache.spark.util.ManualClock")
        .build();


    context = new AppContext(settings, new Configuration());

    JavaSparkContext javaContext = SparkContextFactory.getSparkContext(context.getSettings());


    streamingContext = SparkContextFactory.getStreamingContext(context.getSettings());

    // File tempDir = Files.createTempDir();
    //context.checkpoint(tempDir.toString());

    clock = (ManualClock) streamingContext.ssc().scheduler().clock();

  }

  public static SparkTestHarness load(Class<? extends Application> appClass,
                                   Configuration conf) {

    Application app;

    try {
      app = appClass.newInstance();

    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }

    // Set up the application.
    app.setup(new AppContext(conf));

    return new SparkTestHarness(app, conf);
  }

  /**
   * Runs the stream with the given streams and views.
   */
  public void runStreams(Map<String,List<?>> streams, Map<String,View> views) {

    List<StreamDescription> descriptions = app.getStreamDescriptions();

    for (StreamDescription description: descriptions) {

      Class<? extends StreamingJob> jobClass = description.getJobClass();

      StreamingJob job;

      try {
        job = jobClass.newInstance();
      } catch (InstantiationException e) {
        throw new AppException(e);
      } catch (IllegalAccessException e) {
        throw new AppException(e);
      }

      Method runMethod = JobUtil.resolveRunMethod(job.getClass());

      Map<String,Object> namedArgs = Maps.newHashMap();

      for(Map.Entry<String,List<?>> stream: streams.entrySet()) {

        Queue<JavaRDD<Object>> queue = Queues.newLinkedBlockingQueue();

        JavaRDD rdd = streamingContext.sc().parallelize(stream.getValue());

        queue.add(rdd);

        JavaDStream dstream = streamingContext.queueStream(queue);

        namedArgs.put(stream.getKey(), dstream);
      }

      // Add all views to the named arguments.
      namedArgs.putAll(views);

      Object[] args = JobUtil.getArgs(runMethod, namedArgs);

      // TODO: setup Spark context and population input...

      try {
        runMethod.invoke(job, args);
      } catch (IllegalAccessException e) {
        throw new AppException(e);
      } catch (InvocationTargetException e) {
        throw new AppException(e);
      }

      streamingContext.start();
      clock.advance(120000);

      SparkContextFactory.shutdown();
    }
  }
}
