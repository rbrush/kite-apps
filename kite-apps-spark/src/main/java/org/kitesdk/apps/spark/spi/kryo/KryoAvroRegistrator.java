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
import org.apache.spark.serializer.KryoRegistrator;
import org.kitesdk.apps.AppException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoAvroRegistrator implements KryoRegistrator {

  Logger LOGGER = LoggerFactory.getLogger(KryoAvroRegistrator.class);

  /**
   * System property identifying the Avro classes to be registered
   * with Spark.
   */
  public static final String KITE_AVRO_CLASSES = "kite.avro.classes";

  public void registerClasses(Kryo kryo) {

    String classesString = System.getProperty(KITE_AVRO_CLASSES);

    if (classesString == null || classesString.isEmpty()) {
      LOGGER.info("No Avro classes set in property {}.", KITE_AVRO_CLASSES);
    } else {

      LOGGER.info("Registring Avro classes {}.", classesString);

      String[] classes = classesString.split(",");

      for(String className: classes) {

        Class cls = null;
        try {
          cls = Thread.currentThread().getContextClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
          throw new AppException(e);
        }

        kryo.register(cls, new KryoAvroSerializer(cls));
      }
    }
  }
}
