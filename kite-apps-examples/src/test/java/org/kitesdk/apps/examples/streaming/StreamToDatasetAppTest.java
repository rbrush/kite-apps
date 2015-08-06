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
package org.kitesdk.apps.examples.streaming;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.MiniAppTest;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.spark.test.SparkKafkaTestHarness;
import org.kitesdk.apps.spark.test.SparkTestHarness;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class StreamToDatasetAppTest extends MiniAppTest {

  SparkKafkaTestHarness runner;

  @Before
  public void setup() throws IOException {

    runner = SparkKafkaTestHarness.load(StreamToDatasetApp.class, getConfiguration());
  }

  @After
  public void cleanup() throws IOException {
    runner.tearDown();
  }

  private List<ExampleEvent> getEvents() {

    List<ExampleEvent> events = Lists.newArrayList();

    for (int i = 0; i < 10; ++i) {

      ExampleEvent event = ExampleEvent.newBuilder()
          .setUserId(i)
          .setSessionId(Integer.toString(i))
          .setTimestamp(System.currentTimeMillis())
          .build();

      events.add(event);
    }

    return events;
  }

  @Test
  public void testStream() throws InterruptedException {

    Dataset<ExampleEvent> output = Datasets.load(StreamToDatasetApp.EVENTS_DS_URI, ExampleEvent.class);

    runner.writeMessages(StreamToDatasetApp.TOPIC_NAME, getEvents());

    boolean hasRecords = false;

    for (int i = 0; i < 10; ++i) {

      Thread.sleep(2000);

      // Verify the output contains the expected content.
      DatasetReader<ExampleEvent> reader = output.newReader();

      try {

        int count = 0;

        for (ExampleEvent event: reader) {
          ++count;
        }

        if (count == 10) {
          hasRecords = true;
          break;
        }

      } finally {
        reader.close();
      }
    }

    Assert.assertTrue("Expected output records not found", hasRecords);
  }
}
