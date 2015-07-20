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
package org.kitesdk.apps.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.kitesdk.apps.AppException;
import org.kitesdk.data.View;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.spi.DefaultConfiguration;
import scala.Tuple2;

import java.io.IOException;


/**
 * Helper functions for working with Spark. This might be best refactored
 * into kite-data-spark.
 */
public class SparkDatasets {

  /**
   * Save the contents of the given RDD to the given view.
   *
   * @param rdd
   * @param uri
   */
  public static void save(JavaPairRDD rdd, String uri,  Configuration conf) {

    // Copy configuration to avoid side effects for the caller.
    Configuration outputConf = new Configuration(conf);

    try {
      Job job = Job.getInstance(outputConf);

      DatasetKeyOutputFormat.configure(job).writeTo(uri);

      // Save non-empty RDDs.
      if (!rdd.isEmpty())
        rdd.saveAsNewAPIHadoopDataset(job.getConfiguration());

    } catch (IOException e) {
      throw new AppException(e);
    }
  }


  static final class ToPairFn implements PairFunction {

    @Override
    public Tuple2 call(Object o) throws Exception {
      return new Tuple2(o, null);
    }
  }


  /**
   * Save the contents of the given RDD to the given view.
   *
   * @param rdd
   * @param uri
   */
  public static void save(JavaRDD rdd, String uri) {

    JavaPairRDD pairRDD = rdd.mapToPair(new ToPairFn());

    save(pairRDD, uri, DefaultConfiguration.get());
  }

  /**
   * Save all RDDs in the given DStream to the given view.
   * @param dstream
   * @param view
   */
  public static <T> void save(JavaDStream<T> dstream, final View<T> view) {

    final String uri = view.getUri().toString();

    dstream.foreachRDD(new Function2<JavaRDD<T>, Time, Void>() {
      @Override
      public Void call(JavaRDD<T> rdd, Time time) throws Exception {

        save(rdd, uri);

        return null;
      }
    });
  }

}
