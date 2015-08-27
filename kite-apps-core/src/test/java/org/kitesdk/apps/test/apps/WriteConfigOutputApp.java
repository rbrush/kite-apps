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

import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.test.KeyValues;
import org.kitesdk.data.DatasetDescriptor;

public class WriteConfigOutputApp extends AbstractApplication {

  /**
   * URI of the dataset created by this application.
   */
  public static final String OUTPUT_DATASET = "dataset:hdfs:///tmp/test/keyvalues";

  /**
   * Pattern for output data set, made public for testing purposes.
   */
  public static final String OUTPUT_URI_PATTERN = "view:hdfs:///tmp/test/keyvalues";

  public void setup(AppContext context) {

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(KeyValues.getClassSchema())
        .build();

    dataset(OUTPUT_DATASET, descriptor);

    // Schedule our report to run every five minutes.
    Schedule schedule = new Schedule.Builder()
        .jobName("write-config-job")
        .jobClass(WriteConfigOutputJob.class)
        .frequency("0 * * * *")
        .withOutput("kv-output", OUTPUT_URI_PATTERN)
        .build();

    schedule(schedule);
  }
}
