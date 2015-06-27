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

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.apps.MiniAppTest;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.test.TestScheduler;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;

import java.util.Collections;

/**
 * Test the data generator app.
 */
public class DataGeneratorAppTest extends MiniAppTest {

  @Test
  public void testGenerateData() {

    TestScheduler container = TestScheduler.load(DataGeneratorApp.class, getConfiguration());

    container.runScheduledJobs(new Instant());

    // Verify the expected data was written.
    Dataset<ExampleEvent> ds = Datasets.load(DataGeneratorApp.EVENT_DS_URI, ExampleEvent.class);

    DatasetReader<ExampleEvent> reader = ds.newReader();

    try {

      int count = 0;

      for (ExampleEvent event: reader) {

        ++count;
      }

      Assert.assertEquals(10, count);

    } finally {
      reader.close();
    }
  }
}
