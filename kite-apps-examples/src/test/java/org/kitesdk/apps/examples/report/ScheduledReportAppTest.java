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

import org.apache.avro.generic.GenericData;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.apps.MiniAppTest;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.examples.generate.DataGeneratorApp;
import org.kitesdk.apps.test.TestScheduler;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;

import org.kitesdk.data.View;

import java.util.Collections;


public class ScheduledReportAppTest extends MiniAppTest {

  @Test
  public void testGenerateAndRunReport() {

    DateTime firstNominalTime = new DateTime(2015, 5, 7, 12, 0, 0);

    // Run the generator job at each minute.
    TestScheduler generatorRunner = TestScheduler.load(DataGeneratorApp.class, getConfiguration());

    for (int i = 0; i < 5; ++i) {
      generatorRunner.runScheduledJobs(firstNominalTime.plusMinutes(i).toInstant());
    }

    // Now run the report job to aggregate over the schedule.
    TestScheduler reportRunner = TestScheduler.load(ScheduledReportApp.class, getConfiguration());
    reportRunner.runScheduledJobs(firstNominalTime.plusMinutes(5).toInstant());

    // Verify the expected data was written.
    Dataset<GenericData.Record> ds = Datasets.load(ScheduledReportApp.REPORT_DS_URI, GenericData.Record.class);

    DatasetReader<GenericData.Record> reader = ds.newReader();

    try {

      int count = 0;

      for (GenericData.Record event: reader) {

        // Each had an event created in each generated data run,
        // totalling 5
        Assert.assertEquals(5L, event.get("event_count"));

        ++count;
      }

      // We should see ten distinct IDs.
      Assert.assertEquals(10, count);

    } finally {
      reader.close();
    }
  }
}
