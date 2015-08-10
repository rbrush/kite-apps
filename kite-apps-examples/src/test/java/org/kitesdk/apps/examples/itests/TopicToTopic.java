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

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.examples.streaming.totopic.TopicToTopicApp;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class TopicToTopic {

  public static int main(String[] args) throws IOException {

    if (args.length != 5) {
      System.out.println("Usage: <brokerList> <zookeeperConnect> <environment> <count> <timeout>");
      return 1;
    }

    String brokerList = args[0];
    String zookeeperConnect = args[1];
    String environment = args[2];
    String inputTopic = environment + "." + TopicToTopicApp.INPUT_TOPIC;
    String outputTopic = environment + "." + TopicToTopicApp.OUTPUT_TOPIC;
    int count = Integer.parseInt(args[3]);
    int timeout = Integer.parseInt(args[4]);

    // Open the output stream so we see produced data.
    ConsumerConnector connector = DataUtil.createConnector(zookeeperConnect);

    KafkaStream<byte[],byte[]> stream = DataUtil.openStream(connector, outputTopic);

    // Create a random session ID for testing.
    String sessionId = UUID.randomUUID().toString();

    List<ExampleEvent> events = DataUtil.createEvents(count, sessionId);

    Producer producer = DataUtil.createProducer(brokerList);

    System.out.println("Generating " + count + " messages with session ID " + sessionId);

    DataUtil.sendMessages(inputTopic, producer, events);

    try {

      System.out.println("Checking for expected output.");
      boolean foundMessages = DataUtil.checkMessages(stream, events, timeout);

      if (foundMessages) {
        System.out.println("Found test messages.");
        return 0;
      } else {
        System.out.println("Expected test messages not found");
        System.out.println("Topic: " + outputTopic);
        System.out.println("Session ID: " + sessionId);
        return 1;
      }

    } catch (InterruptedException e) {
      return 1;
    }
  }

}
