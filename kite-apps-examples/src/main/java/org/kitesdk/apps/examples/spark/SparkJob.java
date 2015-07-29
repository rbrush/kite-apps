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
package org.kitesdk.apps.examples.spark;

import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.data.View;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.apps.spark.AbstractSchedulableSparkJob;
import scala.Tuple2;

import java.io.IOException;

/**
 * Simple job that uses a Crunch pipeline to filter out a set of users.
 */
public class SparkJob extends AbstractSchedulableSparkJob {

  @Override
  public String getName() {
    return "example-spark";
  }

  public static class KeepOddUsers implements Function<Tuple2<ExampleEvent, Void>, Boolean> {

    @Override
    public Boolean call(Tuple2<ExampleEvent, Void> input) throws Exception {
      return input._1().getUserId() % 2 == 1;
    }
  }

  public void run(@DataIn(name="example.events", type=ExampleEvent.class) View<ExampleEvent> input,
                  @DataOut(name="odd.users", type=ExampleEvent.class) View<ExampleEvent> output) throws IOException {

    Job job = Job.getInstance(getJobContext().getHadoopConf());
    DatasetKeyInputFormat.configure(job).readFrom(input);
    DatasetKeyOutputFormat.configure(job).writeTo(output);

    JavaPairRDD<ExampleEvent, Void> inputData = getJobContext()
        .getSparkContext()
        .newAPIHadoopRDD(job.getConfiguration(), DatasetKeyInputFormat.class,
            ExampleEvent.class, Void.class);

    JavaPairRDD<ExampleEvent, Void> filteredData = inputData.filter(new KeepOddUsers());

    filteredData.saveAsNewAPIHadoopDataset(job.getConfiguration());
  }
}
