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
package org.kitesdk.apps.examples.generate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;

/**
 * Kite app that creates a dataset and generates data for it.
 */
public class DataGeneratorApp extends AbstractApplication {

  /**
   * URI of the dataset to create.
   */
  public static final String EVENT_DS_URI = "dataset:hive:example/events";

  /**
   * Schedule pattern.
   */
  private final String EVENT_DS_PATTERN = "view:hive:example/events" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}&minute=${MINUTE}";

  @Override
  public void setup(Configuration conf) {

    // Create the test dataset, partitioned by the minute
    // so we quickly create data.
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .hour("timestamp")
        .minute("timestamp")
        .build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(ExampleEvent.getClassSchema())
        .partitionStrategy(strategy)
        .build();

    dataset(EVENT_DS_URI, descriptor);

    // Schedule our data generation job to run every minute. We wire
    // the job's "generate.target" argument to the pattern so it will
    // be invoked with the corresponding view.
    Schedule schedule = new Schedule.Builder()
        .jobClass(DataGeneratorJob.class)
        .frequency("* * * * *")
        .withOutput("example.events", EVENT_DS_PATTERN)
        .build();

    schedule(schedule);
  }
}
