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
package org.kitesdk.apps.spark.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Support for Kafka as a Kite application output.
 */
public class KafkaOutput<T extends SpecificRecord> implements Serializable {

  private static final String PREFIX = "kafka.";
  private static final long serialVersionUID = 2953583026881050675L;

    private static Properties toKafkaProps(Map<String,String> settings) {

    Properties props = new Properties();

    for(Map.Entry<String,String> entry: settings.entrySet()) {

      if (entry.getKey().startsWith(PREFIX)) {
        props.put(entry.getKey().substring(PREFIX.length()),
            entry.getValue());
      }

    }

    return props;
  }

  private String topic;

  private Schema schema;

  private Map<String,String> settings;

  private ProducerPool pool;

  public KafkaOutput(Schema schema, Map<String,String> settings) {

    this.topic = settings.get(Topics.TOPIC_NAME);

    if (topic == null) {
      throw new IllegalArgumentException("Kafka output must have the " + Topics.TOPIC_NAME + " setting set.");
    }

    this.schema = schema;
    this.settings = settings;
    pool = ProducerPool.get(toKafkaProps(settings));
  }

  private void writeObject(ObjectOutputStream out) throws IOException {

    out.writeUTF(topic);
    out.writeUTF(schema.toString());

    out.writeInt(settings.size());
    for (Map.Entry<String,String> entry: settings.entrySet()) {

      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {

    this.topic = in.readUTF();

    String schemaString = in.readUTF();
    this.schema = new Schema.Parser().parse(schemaString);
    this.settings = Maps.newHashMap();

    int size = in.readInt();

    for (int i = 0; i < size; ++i) {

      String key = in.readUTF();
      String value = in.readUTF();

      settings.put(key, value);
    }

    pool = ProducerPool.get(toKafkaProps(settings));
  }

  /**
   * Writes the content of the stream to the Kafka topic
   * behind this producer.
   */
  public void write (JavaDStream<T> stream) {

    stream.foreachRDD(new Function<JavaRDD<T>, Void>() {
      @Override
      public Void call(JavaRDD<T> rdd) throws Exception {

        write(rdd);

        return null;
      }
    });
  }

  class KafkaWriter implements VoidFunction<Iterator<T>> {

    private static final long serialVersionUID = 7056089420805558340L;

    @Override
    public void call(Iterator<T> iterator) throws Exception {

      DatumWriter writer = SpecificData.get().createDatumWriter(schema);

      List<KeyedMessage> messages = Lists.newArrayList();

      while (iterator.hasNext()) {

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        SpecificRecord record = iterator.next();

        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);

        writer.write(record, encoder);
        encoder.flush();

        KeyedMessage<byte[],byte[]> message = new KeyedMessage<byte[], byte[]>(topic, new byte[0], output.toByteArray());

        messages.add(message);
      }

      Producer producer = pool.get();

      producer.send(messages);
    }
  }


  public void write (JavaRDD<T> rdd) {

    rdd.foreachPartition(new KafkaWriter());
  }
}
