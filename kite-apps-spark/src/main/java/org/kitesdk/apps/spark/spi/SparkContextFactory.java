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
package org.kitesdk.apps.spark.spi;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Factor to create Spark contexts from application context. This is necessary
 * since Spark does not yet all multiple contexts in a JVM. See
 * https://issues.apache.org/jira/browse/SPARK-2243 for details.
 */
public class SparkContextFactory {

  private static Map<String,String> appSettings = null;

  private static JavaSparkContext sparkContext = null;

  private static JavaStreamingContext streamingContext = null;

  private static Properties loadSparkDefaults() {
    Properties defaults = new Properties();

    InputStream stream = SparkContextFactory.class.getResourceAsStream("/kite-spark-defaults.properties");

    try {
      defaults.load(stream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {
      Closeables.closeQuietly(stream);
    }

    return defaults;
  }

  private static SparkConf createConf(Map<String,String> settings) {

    SparkConf conf = new SparkConf().setAppName("PLACEHOLDER");

    // Set the defaults first so they may be overwritten.
    Properties defaults = loadSparkDefaults();

    for (String name: defaults.stringPropertyNames()) {

      conf.set(name, defaults.getProperty(name));
    }

    for (Map.Entry<String,String> entry: settings.entrySet()) {

      if (entry.getKey().startsWith("spark.")) {

        conf.set(entry.getKey(), entry.getValue());
      }
    }

    return conf;
  }

  public static synchronized JavaSparkContext getSparkContext(Map<String,String> settings) {

    if (sparkContext == null) {

      appSettings = ImmutableMap.<String,String>builder()
          .putAll(settings)
          .build();

      SparkConf conf = createConf(appSettings);

      sparkContext = new JavaSparkContext(conf);

    } else {

      // Check to see if the settings are compatible.
      if (!appSettings.equals(settings))
        throw new AppException("Can only create a Spark context for one collection of settings. See SPARK-2243.");

    }

    return sparkContext;
  }

  public static synchronized JavaStreamingContext getStreamingContext(Map<String,String> settings) {


    if (streamingContext == null) {

      streamingContext = new JavaStreamingContext(getSparkContext(settings), new Duration(1000));
    } else {

      // Check to see if the settings are compatible.
      if (!appSettings.equals(settings))
        throw new AppException("Can only create a Spark context for one collection of settings. See SPARK-2243.");

    }

    return streamingContext;
  }

  public static synchronized void shutdown() {

    if (streamingContext != null) {
      // FIXME: attempts at a graceful shutdown seem to block
      // indefinitely. Therefore we shut down on a separate thread
      // and timeout so we can make progress on tests.
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {

          streamingContext.stop(true,true);
        }
      });

      thread.setDaemon(true);
      thread.start();

      streamingContext.awaitTermination(5000);
    }

    if (sparkContext != null) {
      sparkContext.stop();
    }

    // Remove the context now that we've been shut down.
    appSettings = null;
    sparkContext = null;
    streamingContext = null;
  }
}
