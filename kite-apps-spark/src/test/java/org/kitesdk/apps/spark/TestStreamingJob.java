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
package org.kitesdk.apps.spark;

import com.clearspring.analytics.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.spark.apps.StreamingSparkApp;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spark.test.SparkKafkaTestHarness;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.View;
import org.kitesdk.data.event.SmallEvent;
import org.kitesdk.data.spi.DefaultConfiguration;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TestStreamingJob extends MiniDFSTest {


  private SparkKafkaTestHarness harness;

  @Before
  public void setupRunner() throws Exception {

    DefaultConfiguration.set(getConfiguration());

    harness = new SparkKafkaTestHarness(StreamingSparkApp.class, getConfiguration());
  }

  @After
  public void cleanupContext() {
    SparkContextFactory.shutdown();
  }

  /**
   * Loads the expected number of items when they are available in the dataset.
   * @param expected
   * @return
   */
  private <T> List<T> loadWhenAvailable(View<T> view, int expected) {

    for (int attempt = 0; attempt < 20; ++attempt) {

      List<T> items = Lists.newArrayList();

      DatasetReader<T> reader = view.newReader();
      int count = 0;

      try {

        for (; count < expected; ++count) {

          if (!reader.hasNext())
            continue;


          items.add(reader.next());
        }

      } finally {
        reader.close();
      }

      if (count == expected) {
        return items;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    Assert.fail("Unable to load the expected items.");
    return null;
  }

  @Test
  public void testJob() throws IOException {

    List<SmallEvent> events = Lists.newArrayList();

    for (int i = 0; i < 10; ++i) {
      SmallEvent event = SmallEvent.newBuilder()
          .setSessionId("1234")
          .setUserId(i)
          .build();

      events.add(event);
    }

    harness.writeMessages(StreamingSparkApp.TOPIC_NAME, events);

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // List<SpecificRecord> records = harness.readMessages(StreamingSparkApp.TOPIC_NAME, SmallEvent.getClassSchema(), 1);
    // System.out.println("RECORDS: " + records);

    View<SmallEvent> view = Datasets.load(StreamingSparkApp.EVENTS_DS_URI, SmallEvent.class);

    List<SmallEvent> results = loadWhenAvailable(view, 10);

    Assert.assertEquals(events, results);
  }

  @After
  public void tearDown() throws IOException {
    harness.tearDown();
  }
}
