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
package org.kitesdk.apps.spark.apps;

import com.google.common.io.Closeables;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitsdk.apps.spark.AbstractSchedulableSparkJob;

import java.io.IOException;

/**
 * Simple Spark example job.
 */
public class SimpleSparkJob extends AbstractSchedulableSparkJob {

  @Override
  public String getName() {
    return "simple-spark-job";
  }

  public void run(@DataIn(name="source.users") View<GenericRecord> input,
                  @DataOut(name="target.users") View<GenericRecord> output) throws IOException {

    Job job = Job.getInstance();
    DatasetKeyInputFormat.configure(job).readFrom(input);
    DatasetKeyOutputFormat.configure(job).writeTo(output);

    @SuppressWarnings("unchecked")
    JavaPairRDD<GenericData.Record, Void> inputData = getContext()
        .newAPIHadoopRDD(job.getConfiguration(), DatasetKeyInputFormat.class,
            GenericData.Record.class, Void.class);

    inputData.saveAsNewAPIHadoopDataset(job.getConfiguration());
  }
}
