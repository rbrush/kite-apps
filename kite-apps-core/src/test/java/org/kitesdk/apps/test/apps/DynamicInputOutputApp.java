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
package org.kitesdk.apps.test.apps;

import org.apache.avro.generic.GenericRecord;
import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;

public class DynamicInputOutputApp extends AbstractApplication {

  public static final String INPUT1_DATASET = "dataset:hdfs:///tmp/test/input1_records";

  public static final String INPUT2_DATASET = "dataset:hdfs:///tmp/test/input2_records";

  /**
   * Pattern to match input data, made public for testing purposes.
   */
  public static final String INPUT1_URI_PATTERN = "view:hdfs:///tmp/test/input1_records" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}";

  /**
   * Pattern to match input data, made public for testing purposes.
   */
  public static final String INPUT2_URI_PATTERN = "view:hdfs:///tmp/test/input2_records" +
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

  @Override
  public void setup(AppContext context) {

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

    // Create the input and output datasets
    dataset(INPUT1_DATASET, descriptor);
    dataset(INPUT2_DATASET, descriptor);
    dataset(OUTPUT_DATASET, descriptor);

    Schedule.Builder builder = new Schedule.Builder()
        .jobName("test_job")
        .jobClass(DynamicInputOutputJob.class)
        .frequency("*/5 * * * *")
        .withInput("input1", INPUT1_URI_PATTERN, "*/5 * * * *", GenericRecord.class)
        .withInput("input2", INPUT2_URI_PATTERN, "*/5 * * * *", GenericRecord.class)
        .withOutput("output", OUTPUT_URI_PATTERN, GenericRecord.class);

    schedule(builder.build());
  }
}
