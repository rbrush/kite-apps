package org.kitesdk.apps.examples.generate;

import org.apache.hadoop.conf.Configuration;
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
        .withView("example.events", EVENT_DS_PATTERN, 1)
        .build();

    schedule(schedule);
  }
}
