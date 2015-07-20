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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.MiniAppTest;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.spark.test.SparkTestHarness;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class SparkStreamingAppTest extends MiniAppTest {

  SparkTestHarness runner;

  @Before
  public void setup() {

    runner = SparkTestHarness.load(SparkStreamingApp.class, getConfiguration());
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
  public void testStream() {

    Map<String,List<?>> inputs = Maps.newHashMap();

    inputs.put("event.stream", getEvents());

    Map<String,View> outputs = Maps.newHashMap();

    Dataset<ExampleEvent> output = Datasets.load(SparkStreamingApp.EVENTS_DS_URI, ExampleEvent.class);

    outputs.put("event.output", output);

    runner.runStreams(inputs, outputs);

    // Verify the output contains the expected content.
    DatasetReader<ExampleEvent> reader = output.newReader();

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
