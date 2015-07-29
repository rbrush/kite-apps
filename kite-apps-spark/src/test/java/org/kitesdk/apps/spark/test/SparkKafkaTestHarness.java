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
package org.kitesdk.apps.spark.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.ManualClock;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.Application;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spark.apps.StreamingSparkApp;
import org.kitesdk.apps.spark.spi.streaming.SparkStreamingJobManager;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.StreamingJobManager;
import org.kitesdk.apps.streaming.StreamDescription;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Support for running spark streaming apps in unit tests.
 */
public class SparkKafkaTestHarness {

  private final Application app;

  private final JavaStreamingContext context;

  private final ManualClock clock;

  private final KafkaBrokerTestHarness harness;

  private final List<StreamingJobManager> managers = Lists.newArrayList();

  private final Producer<byte[],byte[]> producer;

  private final AppContext appContext;

  public SparkKafkaTestHarness(Class cls, Configuration conf) throws IOException {

    // Set up Kafka.
    harness = new KafkaBrokerTestHarness();
    harness.setUp();

    // Create an application context configured to use the Kafka test instance.
    String brokerList = (String) harness.getProducerProps().get("metadata.broker.list");
    String zookeeperString = (String) harness.getConsumerProps().get("zookeeper.connect");

    Map<String,String> props = ImmutableMap.<String,String>builder()
        .put("spark.master", "local[3]")
        .put("spark.app.name", "spark-test")
        .put("spark.streaming.clock", "org.apache.spark.util.ManualClock")
        .put("kafka.metadata.broker.list", brokerList)
        .put("kafka.zookeeper.connect", zookeeperString)
        .build();

    appContext = new AppContext(props, conf);

    context = SparkContextFactory.getStreamingContext(appContext.getSettings());

    File tempDir = Files.createTempDir();
    context.checkpoint(tempDir.toString());

    clock = (ManualClock) context.ssc().scheduler().clock();

    try {
      app = (Application) cls.newInstance();
    } catch (IllegalAccessException e) {

      throw new AppException(e);

    } catch (InstantiationException e) {
      throw new AppException(e);
    }

    // Set up the application
    app.setup(appContext);

    producer = new Producer<byte[],byte[]>(new ProducerConfig(harness.getProducerProps()));

    List<StreamDescription> descriptions = app.getStreamDescriptions();

    for (StreamDescription description: descriptions) {

      SparkStreamingJobManager manager = (SparkStreamingJobManager) JobManagers.createStreaming(description, appContext);

      managers.add(manager);

      manager.run();
    }

    context.start();
  }


  public static SparkKafkaTestHarness load(Class<? extends Application> appClass,
                                           Configuration conf) throws IOException {

    return new SparkKafkaTestHarness(appClass, conf);
  }

  public void writeMessages(String topic, List<? extends SpecificRecord> records) {

    if (records.size() == 0)
      return;

    DatumWriter writer = SpecificData.get().createDatumWriter(records.get(0).getSchema());

    List<KeyedMessage<byte[],byte[]>> messages = Lists.newArrayList();

    for (SpecificRecord record: records) {

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

      try {
        writer.write(record, encoder);
        encoder.flush();
      } catch (IOException e) {
        throw new AssertionError(e);
      }

      byte[] payload = out.toByteArray();
      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[],byte[]>(topic, new byte[0], payload);

      messages.add(message);
    }

    producer.send(messages);

    // Advance time so the application reads data.
    clock.advance(30000);
  }


  public <T extends SpecificRecord> List<T> readMessages(String topic, Class<T> cls, int count) throws IOException {

    List<T> records = Lists.newArrayList();

    Properties props = harness.getConsumerProps();
    props.setProperty("group.id", "test_group");
    props.put("socket.timeout.ms", "500");
    props.put("consumer.id", "test");
    props.put("auto.offset.reset", "smallest");
    ConsumerConnector connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

    Map<String, List<KafkaStream<byte[], byte[]>>> streams =  connector.createMessageStreams(Collections.singletonMap(topic, 1));

    KafkaStream<byte[],byte[]> stream =  streams.get(topic).get(0);

    Iterator it = stream.iterator();

    Schema schema = SpecificData.get().getSchema(cls);

    DatumReader reader = SpecificData.get().createDatumReader(schema);

    for (int i = 0; i < count; ++i) {

      MessageAndMetadata<byte[],byte[]> message = (MessageAndMetadata<byte[], byte[]>) it.next();

      BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(message.message(),  null);

      T record = (T) reader.read(null, binaryDecoder);

      records.add(record);
    }

    return records;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  /**
   * Terminates the test runner, cleaning up all underlying resources.
   */
  public void tearDown() {

    // FIXME: attempts at a graceful shutdown seem to block
    // indefinitely. Therefore we shut down on a separate thread
    // and timeout so we can make progress on tests.
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {

        context.stop(true,true);
      }
    });

    thread.setDaemon(true);
    thread.start();

    context.awaitTermination(5000);

    harness.tearDown();
  }
}
