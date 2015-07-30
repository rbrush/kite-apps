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
package org.kitesdk.apps.examples.triggered;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;

/**
 * Example application that is triggered by the presence of data.
 */
public class TriggeredApp extends AbstractApplication {

  /**
   * Pattern to match input data.
   */
  static final String EVENT_URI_PATTERN = "view:hive:example/events" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}&minute=${MINUTE}";

  /**
   * URI of the dataset created by this application.
   */
  static final String ODD_USER_DS_URI = "dataset:hive:example/odd_users";

  /**
   * Pattern for output data set.
   */
  static final String ODD_USER_URI_PATTERN = "view:hive:example/odd_users" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}&minute=${MINUTE}";

  @Override
  public void setup(AppContext context) {

    // Create a dataset to contain items partitioned by event.
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("year", "int")
        .provided("month", "int")
        .provided("day", "int")
        .provided("hour", "int")
        .provided("minute", "int")
        .build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(ExampleEvent.getClassSchema())
        .partitionStrategy(strategy)
        .build();

    dataset(ODD_USER_DS_URI, descriptor);

    // Schedule our report to run every five minutes.
    Schedule schedule = new Schedule.Builder()
        .jobClass(TriggeredJob.class)
        .frequency("* * * * *")
        .withInput("example_events", EVENT_URI_PATTERN, "* * * * *")
        .withOutput("odd_users", ODD_USER_URI_PATTERN)
        .build();

    schedule(schedule);
  }
}
