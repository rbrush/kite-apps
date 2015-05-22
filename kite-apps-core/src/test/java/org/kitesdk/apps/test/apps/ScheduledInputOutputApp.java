package org.kitesdk.apps.test.apps;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;

/**
 * Minimal app creating a job with a single input and output for testing.
 */
public class ScheduledInputOutputApp extends AbstractApplication {

  public static final String INPUT_DATASET = "dataset:hdfs:///tmp/test/input_records";

  /**
   * Pattern to match input data, made public for testing purposes.
   */
  public static final String INPUT_URI_PATTERN = "view:hdfs:///tmp/test/input_records" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}";

  /**
   * URI of the dataset created by this application.
   */
  public static final String OUTPUT_DATASET = "dataset:hdfs:///tmp/test/output_records";

  /**
   * Pattern for output data set, made public for testing purposes.
   */
  public static final String OUTPUT_URI_PATTERN = "view:hdfs:///tmp/test/output_records" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}";

  public void setup(Configuration conf) {

    // Create the input and output datasets.
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("year", "int")
        .provided("month", "int")
        .provided("day", "int")
        .provided("hour", "int")
        .build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(DatasetTestUtilities.USER_SCHEMA)
        .partitionStrategy(strategy)
        .build();

    dataset(INPUT_DATASET, descriptor);
    dataset(OUTPUT_DATASET, descriptor);

    // Schedule our report to run every five minutes.
    Schedule schedule = new Schedule.Builder()
        .jobClass(ScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .withView("source.users", INPUT_URI_PATTERN, 1)
        .withView("target.users", OUTPUT_URI_PATTERN, 1)
        .build();

    schedule(schedule);
  }
}
