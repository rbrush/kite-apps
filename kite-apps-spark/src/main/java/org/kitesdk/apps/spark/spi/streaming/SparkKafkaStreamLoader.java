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
package org.kitesdk.apps.spark.spi.streaming;

import com.google.common.collect.Maps;
import kafka.serializer.DefaultDecoder;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.spark.SparkJobContext;
import scala.Tuple2;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SparkKafkaStreamLoader {


  static class ToAvroFunction implements Function<Tuple2<byte[],byte[]>, Object> {

    private Schema schema;

    private void writeObject(ObjectOutputStream output) throws IOException {

      output.writeUTF(schema.toString());
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException  {

      schema = new Schema.Parser().parse(input.readUTF());
    }

    public ToAvroFunction(Schema schema) {

      this.schema = schema;
    }

    @Override
    public Object call(Tuple2<byte[], byte[]> kv) throws Exception {

      Decoder decoder = DecoderFactory.get().binaryDecoder(kv._2(), null);
      DatumReader reader = SpecificData.get().createDatumReader(schema);

      return reader.read(null, decoder);
    }
  }

  public JavaDStream load(Schema schema, Map<String, String> properties, JobContext jobContext)  {

    // JavaPairReceiverInputDStream<String, String> stream =  KafkaUtils.createStream(DefaultSparkContext.getStreamingContext(), "foo", "bar", null);

    JavaStreamingContext ctx = ((SparkJobContext) jobContext).getSparkStreamingContext();


    Map<String, String> params = org.kitesdk.apps.spark.KafkaUtils.getDirectStreamParams((SparkJobContext) jobContext);


    Set<String> topics = new HashSet<String>();
    String topic = properties.get(org.kitesdk.apps.spark.KafkaUtils.TOPIC_NAME);
    topics.add(topic);

    // We retry this ourselves since the underlying Kafka system does not retry.
    Exception cause = null;

    for (int i = 0; i < 10; ++i) {

      try {
        JavaPairInputDStream<byte[], byte[]> stream = KafkaUtils.createDirectStream(ctx,
            byte[].class,
            byte[].class,
            DefaultDecoder.class,
            DefaultDecoder.class,
            params,
            topics);

        return stream.map(new ToAvroFunction(schema));

      } catch (Exception e) {
        cause = e;

        try {
          Thread.sleep(1000);
        } catch (InterruptedException interrupted) {
          throw new AppException(e);
        }
      }
    }

    throw new AppException(cause);
  }
}
