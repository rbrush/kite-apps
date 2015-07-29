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
import org.kitesdk.apps.spark.apps.TopicToTopicApp;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spark.test.SparkKafkaTestHarness;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.event.SmallEvent;
import org.kitesdk.data.spi.DefaultConfiguration;

import java.io.IOException;
import java.util.List;

/**
 * Test for a Spark Streaming application with multiple jobs.
 */
public class TopicToTopicJobTest extends MiniDFSTest {

  private SparkKafkaTestHarness harness;

  @Before
  public void setupRunner() throws Exception {

    DefaultConfiguration.set(getConfiguration());

    harness = new SparkKafkaTestHarness(TopicToTopicApp.class, getConfiguration());
  }

  @After
  public void tearDown() throws IOException {
    harness.tearDown();
    SparkContextFactory.shutdown();
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

    harness.writeMessages(TopicToTopicApp.EVENT_TOPIC_NAME, events);

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    List<SmallEvent> results = harness.readMessages(TopicToTopicApp.PROCESSED_TOPIC_NAME, SmallEvent.class, 10);

    Assert.assertEquals(events, results);
  }
}
