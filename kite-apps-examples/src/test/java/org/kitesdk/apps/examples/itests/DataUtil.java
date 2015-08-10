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

import com.google.common.collect.Lists;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.View;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities for integration tests.
 */
class DataUtil {

  static Producer createProducer(String brokerList) {

    Properties props = new Properties();
    props.setProperty("metadata.broker.list", brokerList);
    props.setProperty("request.timeout.ms", "10000");

    // These two properties below are increased from their defaults to help with the case that auto.create.topics.enable is
    // disabled and a test tries to create a topic and immediately write to it
    props.setProperty("retry.backoff.ms", Integer.toString(500));
    props.setProperty("message.send.max.retries", Integer.toString(1000));

    return new Producer(new ProducerConfig(props));
  }

  static void sendMessages(String topic, Producer producer, List<? extends SpecificRecord> records) throws IOException {

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

  static List<ExampleEvent> createEvents(int count, String sessionID) {
    List<ExampleEvent> events = com.clearspring.analytics.util.Lists.newArrayList();

    for (int i = 0; i < count; ++i) {
      ExampleEvent event = ExampleEvent.newBuilder()
          .setSessionId(sessionID)
          .setUserId(i)
          .setTimestamp(System.currentTimeMillis())
          .build();

      events.add(event);
    }

    return events;
  }

  /**
   * Loads the expected number of items when they are available in the dataset.
   */
  private static List<ExampleEvent> loadWhenAvailable(View<ExampleEvent> view,
                                                      int expected,
                                                      int attempts) throws InterruptedException {

    for (int attempt = 0; attempt < attempts; ++attempt) {

      List<ExampleEvent> items = Lists.newArrayList();

      DatasetReader<ExampleEvent> reader = view.newReader();
      int count = 0;

      try {

        while (count < expected && reader.hasNext()) {

          ExampleEvent event = reader.next();

          items.add(event);

          count++;
        }

      } finally {
        reader.close();
      }

      if (count == expected) {
        return items;
      }

      Thread.sleep(1000);

    }

    return null;
  }

  /**
   * Checks if the given view contains the expected messages within a timeout period.
   */
  static boolean checkMessages(View view, List<ExampleEvent> expected, int timeoutSeconds) throws InterruptedException {

    List<ExampleEvent> actual = loadWhenAvailable(view, expected.size(), timeoutSeconds);

    if (actual == null) {
      return false;
    }

    return expected.equals(actual);
  }

  /**
   * Create a consumer to test outputs to topic.
   */
  static ConsumerConnector createConnector(String zookeeperConnect) {

    Properties props = new Properties();

    // TODO: do we need better group or consumer ID here?
    props.setProperty("group.id", "test_group");
    props.put("consumer.id", "test_consumer");
    props.put("zookeeper.connect", zookeeperConnect);
    props.put("socket.timeout.ms", "5000");
    props.put("auto.offset.reset", "smallest");

    return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }

  /**
   * Opens a stream
   */
  static KafkaStream<byte[],byte[]> openStream(ConsumerConnector connector, String outputTopic) {

    Map<String, List<KafkaStream<byte[], byte[]>>> streams =  connector.createMessageStreams(Collections.singletonMap(outputTopic, 1));

    return streams.get(outputTopic).get(0);
  }

  /**
   * Checks if the topic contains the expected messages within a timeout period.
   */
  static boolean checkMessages(KafkaStream<byte[],byte[]> stream, List<ExampleEvent> expected, int timeoutSeconds) throws IOException, InterruptedException {

    List<ExampleEvent> actual = readRecords(stream, expected.size());

    return expected.equals(actual);
  }

  private static List<ExampleEvent> readRecords(KafkaStream<byte[],byte[]> stream, int expected) throws IOException {

    List<ExampleEvent> records = com.clearspring.analytics.util.Lists.newArrayList();

    Iterator it = stream.iterator();

    // Use loader for the expected event to make sure it is visible.
    SpecificData data = new SpecificData(ExampleEvent.class.getClassLoader());

    DatumReader<ExampleEvent> reader = data.createDatumReader(ExampleEvent.getClassSchema());

    for (int i = 0; i < expected; ++i) {

      MessageAndMetadata<byte[],byte[]> message = (MessageAndMetadata<byte[], byte[]>) it.next();

      BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(message.message(),  null);

      ExampleEvent record = reader.read(null, binaryDecoder);

      records.add(record);
    }

    return records;

  }
}
