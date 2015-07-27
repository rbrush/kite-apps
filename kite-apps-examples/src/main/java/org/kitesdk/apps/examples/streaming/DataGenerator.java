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
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.kitesdk.apps.example.event.ExampleEvent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Class to generate data for the streaming example application.
 */
public class DataGenerator {

  public static int main(String [] args) throws IOException {

    if (args.length != 2) {
      System.out.println("Usage: <brokerList> <topic>");
      return 1;
    }

    String brokerList = args[0];
    String topic = args[1];

    Properties props = new Properties();
    props.setProperty("metadata.broker.list", brokerList);
    props.setProperty("request.timeout.ms", "10000");

    // These two properties below are increased from their defaults to help with the case that auto.create.topics.enable is
    // disabled and a test tries to create a topic and immediately write to it
    props.setProperty("retry.backoff.ms", Integer.toString(500));
    props.setProperty("message.send.max.retries", Integer.toString(1000));

    Producer producer = new Producer(new ProducerConfig(props));

    // Produce data indefinitely, we will exit when interrupted.
    while(true) {

      List<ExampleEvent> events = com.clearspring.analytics.util.Lists.newArrayList();

      for (int i = 0; i < 10; ++i) {
        ExampleEvent event = ExampleEvent.newBuilder()
            .setSessionId("1234")
            .setUserId(i)
            .setTimestamp(System.currentTimeMillis())
            .build();

        events.add(event);
      }

      System.out.println("Sending messages to topic " + topic);
      sendMessages(topic, producer, events);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }

    System.out.println("Finished.");

    return 0;
  }

  private static void sendMessages(String topic, Producer producer, List<? extends SpecificRecord> records) throws IOException {

    DatumWriter writer = SpecificData.get().createDatumWriter(records.get(0).getSchema());

    List<KeyedMessage<byte[],byte[]>> messages = Lists.newArrayList();

    for (SpecificRecord record: records) {

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

      writer.write(record, encoder);
      encoder.flush();

      byte[] payload = out.toByteArray();
      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[],byte[]>(topic, new byte[0], payload);

      messages.add(message);
    }

    producer.send(messages);
  }

}
