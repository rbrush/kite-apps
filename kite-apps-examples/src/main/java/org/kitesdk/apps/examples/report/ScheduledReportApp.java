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
package org.kitesdk.apps.examples.report;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;

/**
 * Example application that simply creates a report at a pre-defined
 * interval.
 */
public class ScheduledReportApp extends AbstractApplication {

  static final String EXAMPLE_DS_URI = "dataset:hive:example/events";

  static final String REPORT_DS_URI = "dataset:hive:example/report";

  static final Schema SCHEMA = SchemaBuilder.record("outcomes")
      .fields()
      .requiredLong("user_id")
      .requiredLong("event_count")
      .endRecord();

  public void setup(AppContext context) {

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("year", "int")
        .provided("month", "int")
        .provided("day", "int")
        .provided("hour", "int")
        .provided("minute", "int")
        .build();

    // Create our test dataset.
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(SCHEMA)
        .partitionStrategy(strategy)
        .build();

    dataset(REPORT_DS_URI, descriptor);

    // Schedule our report to run every five minutes.
    Schedule schedule = new Schedule.Builder()
        .jobClass(ScheduledReportJob.class)
        .frequency("*/5 * * * *")
        .build();

    schedule(schedule);
  }
}
