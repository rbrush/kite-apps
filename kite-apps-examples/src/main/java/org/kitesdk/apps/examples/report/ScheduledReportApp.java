package org.kitesdk.apps.examples.report;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.AbstractApplication;
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

  public void setup(Configuration conf) {

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
