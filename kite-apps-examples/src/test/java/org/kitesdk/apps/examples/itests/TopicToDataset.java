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
package org.kitesdk.apps.examples.itests;

import kafka.javaapi.producer.Producer;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.examples.streaming.todataset.TopicToDatasetApp;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.View;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Integration test for the stream-to-dataset application.
 */
public class TopicToDataset {

  public static int main(String[] args) throws IOException {

    if (args.length != 4) {
      System.out.println("Usage: <brokerList> <topic> <count> <timeout>");
      return 1;
    }

    String brokerList = args[0];
    String topic = args[1];
    int count = Integer.parseInt(args[2]);
    int timeout = Integer.parseInt(args[3]);

    // Create a random session ID for testing.
    String sessionId = UUID.randomUUID().toString();

    List<ExampleEvent> events = DataUtil.createEvents(count, sessionId);

    Producer producer = DataUtil.createProducer(brokerList);

    System.out.println("Generating " + count + " messages with session ID " + sessionId);

    DataUtil.sendMessages(topic, producer, events);

    RefinableView<ExampleEvent> dataset = Datasets.load(TopicToDatasetApp.EVENTS_DS_URI, ExampleEvent.class);

    View<ExampleEvent> view = dataset.with("session_id", sessionId);

    try {

      System.out.println("Checking for expected output.");
      boolean foundMessages = DataUtil.checkMessages(view, events, timeout);

      if (foundMessages) {
        System.out.println("Found test messages.");
        return 0;
      } else {
        System.out.println("Expected test messages not found");
        System.out.println("Dataset: " + TopicToDatasetApp.EVENTS_DS_URI);
        System.out.println("Session ID: " + sessionId);
        return 1;
      }

    } catch (InterruptedException e) {
      return 1;
    }
  }
}
