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
package org.kitesdk.apps.spark.spi.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.kitesdk.apps.AppException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class KryoAvroSerializer extends Serializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KryoAvroSerializer.class);

  private final DatumReader reader;
  private final DatumWriter writer;

  public KryoAvroSerializer(Class<? extends SpecificRecord> record) {

    Schema schema = SpecificData.get().getSchema(record);

    reader = SpecificData.get().createDatumReader(schema);
    writer = SpecificData.get().createDatumWriter(schema);
  }


  @Override
  public void write(Kryo kryo, Output output, Object object) {

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteStream, null);

    try {
      writer.write(object, encoder);
      encoder.flush();

      byte[] bytes = byteStream.toByteArray();

      output.writeInt(bytes.length, true);
      output.write(bytes);
      output.flush();

    } catch (IOException e) {
      throw new AppException(e);
    }
  }

  @Override
  public Object read(Kryo kryo, Input input, Class type) {

    int length = input.readInt(true);

    byte[] bytes = input.readBytes(length);

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new AppException(e);
    }
  }
}
