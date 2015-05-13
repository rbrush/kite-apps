package org.kitesdk.apps.examples.triggered;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.AbstractApplication;
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

  public void setup(Configuration conf) {

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
        .withView("example.events", EVENT_URI_PATTERN, 1)
        .withView("odd.users", ODD_USER_URI_PATTERN, 1)
        .build();

    schedule(schedule);
  }
}
