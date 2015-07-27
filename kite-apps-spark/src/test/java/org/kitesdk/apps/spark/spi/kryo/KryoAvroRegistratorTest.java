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
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.event.SmallEvent;

public class KryoAvroRegistratorTest {

  @After
  public void clearRegistration(){

    System.clearProperty(KryoAvroRegistrator.KITE_AVRO_CLASSES);
  }

  @Test
  public void testRegistrator() {

    System.setProperty(KryoAvroRegistrator.KITE_AVRO_CLASSES, SmallEvent.class.getName());

    Kryo kryo = new Kryo();

    new KryoAvroRegistrator().registerClasses(kryo);

    Serializer serializer = kryo.getSerializer(SmallEvent.class);

    Assert.assertNotNull(serializer);

    Output output = new Output(1024);

    SmallEvent testEvent = SmallEvent.newBuilder()
        .setUserId(1)
        .setSessionId("123")
        .build();

    kryo.writeObject(output, testEvent);

    byte [] bytes = output.toBytes();

    Input input = new Input(bytes);

    SmallEvent result = kryo.readObject(input, SmallEvent.class);

    Assert.assertEquals(testEvent, result);
  }
}
